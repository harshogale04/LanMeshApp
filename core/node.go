package core

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p"
	lphost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	tls "github.com/libp2p/go-libp2p/p2p/security/tls"
	"github.com/multiformats/go-multiaddr"
)

//go:embed static/index.html
var staticFiles embed.FS

type wsConn = websocket.Conn

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Node is a running LANMESH instance. Callers (desktop CLI or the
// mobile bridge) get one back from StartNode and call Stop() to tear
// everything down cleanly — this replaces the old main()'s pattern of
// log.Fatal on any startup error and running forever, since a mobile
// host process must never be killed by a library call failing, and
// must be stoppable on command (e.g. the Android foreground service
// being told to shut down).
type Node struct {
	PeerID     string
	httpServer *http.Server
	cancel     context.CancelFunc
	ctx        context.Context
	host       lphost.Host
	seen       *seenCache
}

// ConnectToPeer dials another LANMESH node directly, bypassing mDNS
// discovery. addr must be a full multiaddr including the peer ID, e.g.
// "/ip4/192.168.1.5/tcp/45343/p2p/12D3KooW...". Use this when mDNS is
// blocked by network isolation (hotspots, campus Wi-Fi).
func (n *Node) ConnectToPeer(addr string) error {
	log.Printf("[LANMESH] ConnectToPeer manual/QR input address: %s", addr)

	maddr, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		return fmt.Errorf("invalid address: %w", err)
	}

	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return fmt.Errorf("invalid peer address: %w", err)
	}

	if info.ID == n.host.ID() {
		return fmt.Errorf("cannot connect to self (%s)", info.ID.String()[:8])
	}

	// Filter out any loopback or unroutable wildcard addresses from the target info
	var cleanAddrs []multiaddr.Multiaddr
	for _, a := range info.Addrs {
		s := a.String()
		if strings.Contains(s, "/127.0.0.1/") || strings.Contains(s, "/::1/") || strings.Contains(s, "/0.0.0.0/") {
			log.Printf("[LANMESH] ConnectToPeer: rejected non-routable dial address for peer %s: %s", info.ID.String()[:8], s)
			continue
		}
		cleanAddrs = append(cleanAddrs, a)
	}

	if len(cleanAddrs) == 0 {
		return fmt.Errorf("cannot connect: address contains loopback/unroutable IP (127.0.0.1); remote peer must provide its LAN address")
	}

	cleanInfo := peer.AddrInfo{
		ID:    info.ID,
		Addrs: cleanAddrs,
	}

	// Clear any stale addresses from peerstore before adding the verified addresses
	n.host.Peerstore().ClearAddrs(cleanInfo.ID)
	n.host.Peerstore().AddAddrs(cleanInfo.ID, cleanInfo.Addrs, peerstore.TempAddrTTL)

	beforeAddrs := n.host.Peerstore().Addrs(cleanInfo.ID)
	log.Printf("[LANMESH] ConnectToPeer: peerstore addresses for %s immediately before dialing: %v", cleanInfo.ID.String()[:8], beforeAddrs)

	ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
	defer cancel()

	return n.host.Connect(ctx, cleanInfo)
}

// ListenAddrsString returns this node's own dialable addresses, one per
// line, each already including "/p2p/<peerID>" so it can be pasted
// straight into another device's ConnectToPeer. Loopback (127.0.0.1) and
// wildcard (0.0.0.0) addresses are strictly excluded so they are never
// shown in QR codes or copied to another physical phone.
func (n *Node) ListenAddrsString() string {
	var parts []string
	for _, a := range n.host.Addrs() {
		s := a.String()
		if strings.Contains(s, "/127.0.0.1/") || strings.Contains(s, "/::1/") || strings.Contains(s, "/0.0.0.0/") {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s/p2p/%s", s, n.PeerID))
	}
	return strings.Join(parts, "\n")
}

func (n *Node) PeerList() []string {
	var ids []string
	for _, p := range n.host.Network().Peers() {
		ids = append(ids, p.String())
	}
	return ids
}

func (n *Node) Stop() error {
	n.cancel()

	var err error

	if n.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if e := n.httpServer.Shutdown(ctx); e != nil {
			n.httpServer.Close()
			err = e
		}
	}

	if n.host != nil {
		n.host.Close()
	}

	dbMu.Lock()
	if db != nil {
		db.Close()
		db = nil
	}
	dbMu.Unlock()

	// Close admin log file
	adminMu.Lock()
	if adminStor != nil {
		if e := adminStor.close(); e != nil && err == nil {
			err = e
		}
		adminStor = nil
	}
	adminMu.Unlock()

	return err
}

// StartNode brings up one LANMESH node: bbolt store, libp2p host, mDNS
// discovery, admin-gated sync, and the HTTP/WebSocket server — then
// returns immediately (the HTTP server runs in its own goroutine).
// Startup failures are returned as errors rather than log.Fatal, since
// a library must never kill its host process.
func StartNode(port int, dataDir string) (*Node, error) {
	if err := ensureDir(dataDir); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}
	setAdminDataDir(dataDir)

	ctx, cancel := context.WithCancel(context.Background())

	dbPath := filepath.Join(dataDir, "lanmesh.db")
	if err := initStore(dbPath); err != nil {
		cancel()
		return nil, err
	}

	// TCP-only: QUIC/UDP is frequently blocked by Android hotspot NAT, AP
	// client isolation, and consumer routers, causing silent 8-second dial
	// timeouts before the stack gives up. TCP is more reliably traversable
	// on typical LAN/hotspot setups.
	listenAddrs := []string{
		"/ip4/0.0.0.0/tcp/0",
	}

	host, err := libp2p.New(
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.Security(tls.ID, tls.New),
		libp2p.AddrsFactory(filterAndExpandAddrs),
	)
	if err != nil {
		cancel()
		return nil, err
	}

	// Diagnostic: log PeerID, actual kernel listening sockets, and advertised addresses at startup.
	// On Android these appear in logcat under tag "GoLog".
	log.Printf("[LANMESH] PeerID: %s", host.ID().String())
	for _, la := range host.Network().ListenAddresses() {
		log.Printf("[LANMESH] Bound listening socket: %s", la.String())
	}
	if addrs := host.Addrs(); len(addrs) == 0 {
		log.Printf("[LANMESH] WARNING: no advertised addresses — peers cannot dial us")
	} else {
		for _, a := range addrs {
			log.Printf("[LANMESH] Advertising: %s/p2p/%s", a.String(), host.ID().String())
		}
	}

	h := newHub()
	nodeSeen := newSeenCache(5000)

	host.SetStreamHandler(protocolID, func(s network.Stream) {
		defer s.Close()
		remote := s.Conn().RemotePeer()

		// Limit incoming envelope size to 2MB to prevent unbounded memory allocation
		var msg ChatMessage
		data, err := io.ReadAll(io.LimitReader(s, 2*1024*1024))
		if err != nil {
			return
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}

		// Signaling separation: call signals must NEVER be accepted, recorded,
		// or relayed over the chat protocol.
		if isCallSignal(msg.Type) {
			log.Printf("[LANMESH] Rejected call signaling type %q on chat protocol %s", msg.Type, protocolID)
			return
		}

		// Deduplicate and suppress loops
		if msg.MessageID == "" || nodeSeen.markSeen(msg.MessageID) {
			return
		}

		// Validate original sender peer ID
		if msg.Sender == "" {
			return
		}
		if _, err := peer.Decode(msg.Sender); err != nil {
			log.Printf("[LANMESH] Dropped message with invalid sender peer ID: %v", err)
			return
		}

		// Hop limit enforcement:
		// Over the wire, any valid message must have 1 <= Hops <= maxHops (6).
		// Hops < 1 indicates unincremented/spoofed origin; Hops > maxHops exceeds the 6-hop boundary.
		if msg.Hops < 1 || msg.Hops > maxHops {
			log.Printf("[LANMESH] Dropped message with out-of-bounds hops (%d, max %d)", msg.Hops, maxHops)
			return
		}

		// Direct hop origin validation:
		// If Hops == 1 (direct neighbor), the transport-level remote peer must match msg.Sender.
		if msg.Hops == 1 && remote.String() != msg.Sender {
			log.Printf("[LANMESH] Dropped spoofed direct message: remote %s != sender %s", remote.String()[:8], msg.Sender[:8])
			return
		}

		stats.record(msg.Hops)
		if err := saveMessage(msg); err != nil {
			log.Println("failed to persist incoming message:", err)
		}
		h.broadcast(msg)
		// Multi-hop mesh forwarding: all connected mesh nodes forward messages
		// towards other peers (up to maxHops=6), not just admin!
		relayToPeers(ctx, host, remote, msg)
		if store := currentAdminStore(); store != nil {
			store.record(msg)
		}
	})

	applyBackfill := func(msg ChatMessage) {
		if msg.MessageID == "" || nodeSeen.markSeen(msg.MessageID) {
			return
		}
		if msg.Hops < 0 || msg.Hops > maxHops {
			return
		}
		wasNewGroup := !groupKnownLocally(msg.GroupID)
		stats.record(msg.Hops)
		if err := saveMessage(msg); err != nil {
			log.Println("failed to persist backfilled message:", err)
		}
		h.broadcast(msg)
		if wasNewGroup && msg.GroupID != "general" {
			h.broadcast(ChatMessage{
				Type:    "group_announce",
				Payload: msg.GroupID,
				GroupID: msg.GroupID,
			})
		}
		relayToPeers(ctx, host, "", msg)
		if store := currentAdminStore(); store != nil {
			store.record(msg)
		}
	}

	host.SetStreamHandler(syncProtocolID, func(s network.Stream) {
		syncServer(s, applyBackfill)
	})

	host.Network().Notify(newSyncNotifee(ctx, host, applyBackfill, isNetworkAdmin))

	// Direct call signaling handler.
	// Delivers call signals to this device's connected WebSocket clients
	// only. Call signals are not persisted, counted, or relayed.
	host.SetStreamHandler(callProtocolID, func(s network.Stream) {
		defer s.Close()

		data, err := io.ReadAll(io.LimitReader(s, 2*1024*1024))
		if err != nil {
			log.Printf("[LANMESH-CALL] Error reading call stream from %s: %v", s.Conn().RemotePeer().String()[:8], err)
			return
		}

		var msg ChatMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("[LANMESH-CALL] Error unmarshaling call signal: %v", err)
			return
		}

		remoteShort := s.Conn().RemotePeer().String()
		if len(remoteShort) > 8 {
			remoteShort = remoteShort[:8]
		}
		log.Printf("[LANMESH-CALL] Incoming %s from %s (group: %q)", msg.Type, remoteShort, msg.GroupID)

		// Deliver straight to this device's own UI only — see isCallSignal's
		// comment for why this never touches saveMessage/stats/relayToPeers.
		h.broadcast(msg)
	})

	if err := setupMDNS(ctx, host); err != nil {
		log.Printf("[LANMESH] mDNS unavailable at startup (%v) — will retry every 10s", err)
		go func() {
			t := time.NewTicker(10 * time.Second)
			defer t.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if err := setupMDNS(ctx, host); err != nil {
						log.Printf("[LANMESH] mDNS retry failed: %v", err)
					} else {
						log.Printf("[LANMESH] mDNS discovery started (retry succeeded)")
						return
					}
				}
			}
		}()
	} else {
		log.Printf("[LANMESH] mDNS discovery started successfully")
	}

	// Peer Exchange: gossip known peer multiaddresses over direct connections.
	// Fixes Android hotspot mDNS multicast isolation: when Phone A connects to
	// Phone B via QR code, they exchange peer tables so each can learn and dial
	// Phone C (who they didn't know via mDNS).
	setupPEX(ctx, host)

	mux := http.NewServeMux()
	registerAdminRoutes(mux)

	staticSub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		host.Close()
		cancel()
		return nil, err
	}
	mux.Handle("/", http.FileServer(http.FS(staticSub)))

	mux.HandleFunc("/peers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		type peerInfo struct {
			ID    string   `json:"id"`
			Addrs []string `json:"addrs"`
		}
		peerList := make([]peerInfo, 0)
		for _, p := range host.Network().Peers() {
			addrStrs := make([]string, 0)
			for _, conn := range host.Network().ConnsToPeer(p) {
				addrStrs = append(addrStrs, conn.RemoteMultiaddr().String())
			}
			peerList = append(peerList, peerInfo{ID: p.String(), Addrs: addrStrs})
		}

		json.NewEncoder(w).Encode(map[string]interface{}{
			"self":  host.ID().String(),
			"peers": peerList,
		})
	})

	// /debug/addrs returns our own advertised multiaddresses (after filterAndExpandAddrs).
	// Use: curl http://localhost:9000/debug/addrs from adb shell or phone browser.
	// Confirms whether the phone is advertising a real LAN IP or 0.0.0.0.
	mux.HandleFunc("/debug/addrs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		addrStrs := make([]string, 0)
		for _, a := range host.Addrs() {
			addrStrs = append(addrStrs, fmt.Sprintf("%s/p2p/%s", a.String(), host.ID().String()))
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"self":  host.ID().String(),
			"addrs": addrStrs,
		})
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		messagesSeen, maxHops, lastActivity := stats.snapshot()
		lastActivitySecs := -1.0

		if !lastActivity.IsZero() {
			lastActivitySecs = time.Since(lastActivity).Seconds()
		}

		json.NewEncoder(w).Encode(map[string]interface{}{
			"connectedPeers":           len(host.Network().Peers()),
			"messagesSeen":             messagesSeen,
			"maxHopsObserved":          maxHops,
			"secondsSinceLastActivity": lastActivitySecs,
			"isAdmin":                  isNetworkAdmin(),
		})
	})

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("WebSocket upgrade failed:", err)
			return
		}
		defer conn.Close()

		h.add(conn)
		defer h.remove(conn)

		conn.WriteJSON(ChatMessage{
			Type:    "whoami",
			Payload: host.ID().String(),
		})

		for _, g := range knownGroups() {
			if g == "general" {
				continue
			}
			conn.WriteJSON(ChatMessage{
				Type:    "group_announce",
				Payload: g,
				GroupID: g,
			})
		}

		for _, g := range knownGroups() {
			for _, m := range historyFor(g) {
				conn.WriteJSON(m)
			}
		}

		for {
			var msg ChatMessage
			if err := conn.ReadJSON(&msg); err != nil {
				break
			}

			msg.Sender = host.ID().String()

			// Handle call signaling before normal chat processing.
			if isCallSignal(msg.Type) {
				msg.Timestamp = time.Now().UnixMilli()
				if msg.To == "" {
					// Broadcast room call signal to all directly connected peers
					connectedPeers := host.Network().Peers()
					log.Printf("[LANMESH-CALL] Broadcasting %s signal (group: %q, active peers: %d)", msg.Type, msg.GroupID, len(connectedPeers))
					data, err := json.Marshal(msg)
					if err == nil {
						for _, p := range connectedPeers {
							go func(target peer.ID) {
								sctx, scancel := context.WithTimeout(ctx, 5*time.Second)
								defer scancel()
								stream, err := host.NewStream(sctx, target, callProtocolID)
								if err != nil {
									log.Printf("[LANMESH-CALL] Failed to open broadcast stream to %s: %v", target.String()[:8], err)
									return
								}
								defer stream.Close()
								if _, err := stream.Write(data); err != nil {
									stream.Reset()
									log.Printf("[LANMESH-CALL] Failed to write broadcast to %s: %v", target.String()[:8], err)
								}
							}(p)
						}
					}
					continue
				}

				targetID, err := peer.Decode(msg.To)
				if err != nil {
					log.Println("[LANMESH-CALL] Bad target peer id:", err)
					continue
				}

				targetShort := targetID.String()
				if len(targetShort) > 8 {
					targetShort = targetShort[:8]
				}
				log.Printf("[LANMESH-CALL] Sending %s directly to %s (group: %q)", msg.Type, targetShort, msg.GroupID)

				sctx, scancel := context.WithTimeout(ctx, 8*time.Second)
				if err := sendDirect(sctx, host, targetID, msg); err != nil {
					log.Printf("[LANMESH-CALL] Direct signal delivery to %s failed: %v", targetShort, err)
				}
				scancel()

				continue
			}

			if msg.GroupID == "" {
				msg.GroupID = "general"
			}

			msg.MessageID = newMessageID()
			msg.Hops = 0
			msg.Timestamp = time.Now().UnixMilli()

			nodeSeen.markSeen(msg.MessageID)
			markSeen(msg.MessageID)
			stats.record(msg.Hops)

			if err := saveMessage(msg); err != nil {
				log.Println("failed to persist outgoing message:", err)
			}

			h.broadcast(msg)
			relayToPeers(ctx, host, "", msg)

			if store := currentAdminStore(); store != nil {
				store.record(msg)
			}
		}
	})

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Println("HTTP server error:", err)
		}
	}()

	// Start SubnetScanner for automatic cold-start peer discovery on local /24 subnets.
	// Operates over unicast TCP/HTTP so discovery functions even when Android hotspot
	// blocks client-to-client mDNS multicast.
	if port > 0 {
		startSubnetScanner(ctx, host, port)
	}

	go func() {
		<-ctx.Done()
		host.Close()
	}()

	return &Node{
		PeerID:     host.ID().String(),
		httpServer: srv,
		cancel:     cancel,
		ctx:        ctx,
		host:       host,
		seen:       nodeSeen,
	}, nil
}

func ensureDir(dir string) error {
	return dirMkdirAll(dir)
}

var (
	configuredLocalIPMu sync.RWMutex
	configuredLocalIP   string
)

// SetLocalIP allows the Android mobile bridge (or CLI) to register the actual
// local LAN IP address obtained directly from Android's ConnectivityManager.
func SetLocalIP(ip string) {
	configuredLocalIPMu.Lock()
	defer configuredLocalIPMu.Unlock()
	configuredLocalIP = strings.TrimSpace(ip)
	log.Printf("[LANMESH] Configured local LAN IP updated to: %s", configuredLocalIP)
}

func GetConfiguredLocalIP() string {
	configuredLocalIPMu.RLock()
	defer configuredLocalIPMu.RUnlock()
	return configuredLocalIP
}

// findLANIPs discovers real, dialable IPv4 addresses for the local device.
// It uses four independent methods to guarantee finding the address even on
// modern Android versions where netlink queries may be blocked by SELinux:
//  1. Configured local IP (from Android ConnectivityManager via SetLocalIP)
//  2. net.Interfaces()
//  3. net.InterfaceAddrs()
//  4. UDP routing probe (queries the kernel routing table without sending traffic)
func findLANIPs() []net.IP {
	var lanIPs []net.IP
	seen := make(map[string]bool)

	addIP := func(ip net.IP, source string) {
		if ip == nil {
			return
		}
		ipv4 := ip.To4()
		if ipv4 == nil || ipv4.IsLoopback() || ipv4.IsUnspecified() {
			return
		}
		s := ipv4.String()
		if !seen[s] {
			seen[s] = true
			lanIPs = append(lanIPs, ipv4)
			log.Printf("[LANMESH] Found LAN interface IP via %s: %s", source, s)
		}
	}

	// 1. Android ConnectivityManager / user-configured IP
	if cfg := GetConfiguredLocalIP(); cfg != "" {
		if parsed := net.ParseIP(cfg); parsed != nil {
			addIP(parsed, "SetLocalIP")
		}
	}

	// 2. net.Interfaces()
	if ifaces, err := net.Interfaces(); err == nil {
		for _, ifi := range ifaces {
			if (ifi.Flags&net.FlagUp) == 0 || (ifi.Flags&net.FlagLoopback) != 0 {
				continue
			}
			addrsList, err := ifi.Addrs()
			if err != nil {
				continue
			}
			for _, a := range addrsList {
				var ip net.IP
				switch v := a.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
				}
				addIP(ip, fmt.Sprintf("net.Interfaces(%s)", ifi.Name))
			}
		}
	} else {
		log.Printf("[LANMESH] net.Interfaces() error: %v", err)
	}

	// 3. net.InterfaceAddrs()
	if ifAddrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range ifAddrs {
			if ipnet, ok := a.(*net.IPNet); ok {
				addIP(ipnet.IP, "net.InterfaceAddrs")
			}
		}
	}

	// 4. UDP routing probe: connect() on a UDP socket asks the kernel routing
	// table which local IP would route outbound, without sending any packets.
	probeTargets := []string{
		"224.0.0.1:9000",       // Multicast subnet
		"255.255.255.255:9000", // Broadcast
		"1.1.1.1:80",           // Gateway
	}
	for _, target := range probeTargets {
		conn, err := net.DialTimeout("udp4", target, 500*time.Millisecond)
		if err == nil {
			if udpAddr, ok := conn.LocalAddr().(*net.UDPAddr); ok {
				addIP(udpAddr.IP, fmt.Sprintf("UDP probe(%s)", target))
			}
			conn.Close()
		}
	}

	return lanIPs
}

// filterAndExpandAddrs replaces 0.0.0.0 listen addresses with real, dialable local
// LAN interface IPv4 addresses so that other nodes on the Wi-Fi/hotspot network can
// connect to this node. It strictly excludes loopback (127.0.0.1) and unroutable addresses.
// It will NEVER return loopback addresses or fall back to 0.0.0.0.
func filterAndExpandAddrs(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
	lanIPs := findLANIPs()

	var result []multiaddr.Multiaddr
	seen := make(map[string]bool)

	for _, a := range addrs {
		str := a.String()
		// Never advertise loopback
		if strings.Contains(str, "/ip4/127.0.0.1/") || strings.Contains(str, "/ip6/::1/") {
			log.Printf("[LANMESH] filterAndExpandAddrs: skipped loopback addr: %s", str)
			continue
		}

		if strings.Contains(str, "/ip4/0.0.0.0/") {
			for _, ip := range lanIPs {
				expandedStr := strings.Replace(str, "/ip4/0.0.0.0/", fmt.Sprintf("/ip4/%s/", ip.String()), 1)
				if m, err := multiaddr.NewMultiaddr(expandedStr); err == nil {
					if !seen[m.String()] {
						seen[m.String()] = true
						result = append(result, m)
						log.Printf("[LANMESH] addr expanded: %s -> %s", str, m.String())
					}
				}
			}
		} else {
			if !seen[str] {
				seen[str] = true
				result = append(result, a)
			}
		}
	}

	if len(result) == 0 {
		log.Printf("[LANMESH] WARNING: filterAndExpandAddrs found no routable LAN addresses (lan IPs: %d)", len(lanIPs))
		return nil
	}
	return result
}