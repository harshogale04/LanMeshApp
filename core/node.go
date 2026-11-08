package core

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	lphost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	tls "github.com/libp2p/go-libp2p/p2p/security/tls"
	"github.com/multiformats/go-multiaddr"
	bolt "go.etcd.io/bbolt"
)

//go:embed static/index.html
var staticFiles embed.FS

type wsConn = websocket.Conn

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type NodeConfig struct {
	Port        int
	P2PPort     int
	Nickname    string
	DataDir     string
	AdminSecret string
	NoBrowser   bool
}

// NodeOptions is the startup configuration for a LANMESH daemon node.
// It is an alias of NodeConfig so both names keep working.
type NodeOptions = NodeConfig

// Node is a running LANMESH instance.
type Node struct {
	PeerID     string
	Nickname   string
	IsAdmin    bool
	httpServer *http.Server
	cancel     context.CancelFunc
	ctx        context.Context
	host       lphost.Host
	seen       *seenCache
	beacon     *BeaconManager
}

type peerRosterInfo struct {
	Nickname string `json:"nickname"`
	Seen     int64  `json:"seen"`
}

var (
	rosterMu sync.RWMutex
	roster   = make(map[string]peerRosterInfo)
)

func updateRoster(peerID, nickname string) {
	if peerID == "" {
		return
	}
	rosterMu.Lock()
	defer rosterMu.Unlock()
	entry := roster[peerID]
	if nickname != "" {
		entry.Nickname = nickname
	}
	entry.Seen = time.Now().UnixMilli()
	roster[peerID] = entry
}

func getRosterSnapshot() map[string]peerRosterInfo {
	rosterMu.RLock()
	defer rosterMu.RUnlock()
	cp := make(map[string]peerRosterInfo, len(roster))
	for k, v := range roster {
		cp[k] = v
	}
	return cp
}

func loadOrGenerateKey(keyPath string) (crypto.PrivKey, error) {
	if data, err := os.ReadFile(keyPath); err == nil {
		priv, err := crypto.UnmarshalPrivateKey(data)
		if err == nil {
			return priv, nil
		}
	}
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}
	data, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, err
	}
	_ = os.WriteFile(keyPath, data, 0600)
	return priv, nil
}

// ConnectToPeer dials another LANMESH node directly, bypassing mDNS discovery.
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

	n.host.Peerstore().ClearAddrs(cleanInfo.ID)
	n.host.Peerstore().AddAddrs(cleanInfo.ID, cleanInfo.Addrs, peerstore.TempAddrTTL)

	ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
	defer cancel()

	return n.host.Connect(ctx, cleanInfo)
}

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

	if n.beacon != nil {
		n.beacon.Stop()
	}

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

// loadOrCreateIdentity loads (or creates and persists) the node's Ed25519
// identity key from <dataDir>/peer.key, logging if an existing key is corrupted.
func loadOrCreateIdentity(dataDir string) (crypto.PrivKey, error) {
	keyPath := filepath.Join(dataDir, "peer.key")
	if data, err := os.ReadFile(keyPath); err == nil {
		priv, err := crypto.UnmarshalPrivateKey(data)
		if err == nil {
			return priv, nil
		}
		log.Printf("[LANMESH] Corrupted %s, generating fresh key: %v", keyPath, err)
	}

	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to generate ed25519 key: %w", err)
	}

	data, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, err
	}

	if err := os.WriteFile(keyPath, data, 0600); err != nil {
		log.Printf("[LANMESH] Warning: failed to persist %s: %v", keyPath, err)
	}
	return priv, nil
}

// StartNode brings up one LANMESH node with default options.
func StartNode(port int, dataDir string) (*Node, error) {
	return StartNodeWithConfig(NodeConfig{
		Port:    port,
		DataDir: dataDir,
	})
}

// StartNodeWithOptions brings up one LANMESH node according to NodeOptions.
func StartNodeWithOptions(opts NodeOptions) (*Node, error) {
	return StartNodeWithConfig(opts)
}

func StartNodeWithConfig(cfg NodeConfig) (*Node, error) {
	if cfg.DataDir == "" {
		cfg.DataDir = "./lanmesh-data"
	}
	if err := ensureDir(cfg.DataDir); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	// Admin secret precedence: explicit config > LANMESH_ADMIN_SECRET env > <dataDir>/admin.secret
	if cfg.AdminSecret == "" {
		cfg.AdminSecret = strings.TrimSpace(os.Getenv("LANMESH_ADMIN_SECRET"))
	}
	if cfg.AdminSecret == "" {
		if secretData, err := os.ReadFile(filepath.Join(cfg.DataDir, "admin.secret")); err == nil {
			cfg.AdminSecret = strings.TrimSpace(string(secretData))
		}
	}
	SetAdminConfig(cfg.AdminSecret, cfg.DataDir, cfg.Nickname)
	ctx, cancel := context.WithCancel(context.Background())

	dbPath := filepath.Join(cfg.DataDir, "lanmesh.db")
	if err := initStore(dbPath); err != nil {
		cancel()
		return nil, err
	}

	// Persist / load Ed25519 identity key
	privKey, err := loadOrCreateIdentity(cfg.DataDir)
	if err != nil {
		cancel()
		return nil, err
	}

	// TCP-only: QUIC/UDP is frequently blocked by Android hotspot NAT, AP
	// client isolation, and consumer routers, causing silent 8-second dial
	// timeouts before the stack gives up. TCP is more reliably traversable
	// on typical LAN/hotspot setups.
	p2pListenPort := 0
	if cfg.P2PPort > 0 {
		p2pListenPort = cfg.P2PPort
	}

	var opts []libp2p.Option
	opts = append(opts,
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", p2pListenPort)),
	)

	opts = append(opts,
		libp2p.Security(tls.ID, tls.New),
		libp2p.AddrsFactory(filterAndExpandAddrs),
	)

	host, err := libp2p.New(opts...)
	if err != nil {
		cancel()
		return nil, err
	}

	log.Printf("[LANMESH] PeerID: %s", host.ID().String())
	for _, la := range host.Network().ListenAddresses() {
		log.Printf("[LANMESH] Bound listening socket: %s", la.String())
	}
	for _, a := range host.Addrs() {
		log.Printf("[LANMESH] Advertising: %s/p2p/%s", a.String(), host.ID().String())
	}

	h := newHub()
	nodeSeen := newSeenCache(5000)

	// If this node is an authoritative admin node, activate audit logging and backfill immediately
	if IsAdminNode() {
		if store, err := enableAdminLogging(); err != nil {
			log.Printf("[LANMESH-ADMIN] Warning: failed to enable boot-time admin logging: %v", err)
		} else if store != nil {
			log.Printf("[LANMESH-ADMIN] Boot-time authoritative admin node active")
			dbMu.RLock()
			currentDB := db
			dbMu.RUnlock()
			if currentDB != nil {
				_ = store.backfillFromDB(currentDB)
			}
		}
	}

	host.SetStreamHandler(protocolID, func(s network.Stream) {
		defer s.Close()
		remote := s.Conn().RemotePeer()

		var msg ChatMessage
		data, err := io.ReadAll(io.LimitReader(s, 2*1024*1024))
		if err != nil {
			return
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}

		if isCallSignal(msg.Type) {
			log.Printf("[LANMESH] Rejected call signaling type %q on chat protocol %s", msg.Type, protocolID)
			return
		}

		if msg.MessageID == "" || nodeSeen.markSeen(msg.MessageID) {
			return
		}

		if msg.Sender == "" {
			return
		}
		if _, err := peer.Decode(msg.Sender); err != nil {
			log.Printf("[LANMESH] Dropped message with invalid sender peer ID: %v", err)
			return
		}

		// Over the wire: 1 <= Hops <= maxHops (6)
		if msg.Hops < 1 || msg.Hops > maxHops {
			log.Printf("[LANMESH] Dropped message with out-of-bounds hops (%d, max %d)", msg.Hops, maxHops)
			return
		}

		if msg.Hops == 1 && remote.String() != msg.Sender {
			log.Printf("[LANMESH] Dropped spoofed direct message: remote %s != sender %s", remote.String()[:8], msg.Sender[:8])
			return
		}

		if len(msg.HopTrail) == 0 {
			msg.HopTrail = []string{msg.Sender}
		}

		stats.record(msg.Hops)
		updateRoster(msg.Sender, msg.Nickname)

		if msg.Type == "ping" {
			h.broadcast(msg)
			relayToPeers(ctx, host, remote, msg)
			return
		}

		if err := saveMessage(msg); err != nil {
			log.Println("failed to persist incoming message:", err)
		}
		h.broadcast(msg)
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
		updateRoster(msg.Sender, msg.Nickname)
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
		// Zero Live-Relay Echo: Synced historical messages are NOT forwarded with relayToPeers
		if store := currentAdminStore(); store != nil {
			store.record(msg)
		}
	}

	host.SetStreamHandler(syncProtocolID, func(s network.Stream) {
		handleSyncStream(s)
	})
	host.SetStreamHandler(legacySyncProtocol, func(s network.Stream) {
		handleSyncStream(s)
	})

	host.Network().Notify(newSyncNotifee(ctx, host, applyBackfill))

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

	setupPEX(ctx, host)

	// UDP LAN Broadcast Beacon (:9999): robust fallback discovery for Android
	// API 30+ where SELinux sandbox blocks netlink route-table inspection.
	beaconMgr, _ := startBeaconEngine(ctx, host)

	// Automated Admin Presence Self-Ping: broadcast a ping frame every 20 seconds
	// to keep the admin node active in rosters across the mesh
	if IsAdminNode() {
		go func() {
			ticker := time.NewTicker(20 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					pingMsg := ChatMessage{
						Type:      "ping",
						Payload:   "admin-ping",
						Sender:    host.ID().String(),
						Nickname:  GetDefaultNickname(),
						GroupID:   "general",
						MessageID: newMessageID(),
						Hops:      0,
						HopTrail:  []string{host.ID().String()},
						Timestamp: time.Now().UnixMilli(),
					}
					nodeSeen.markSeen(pingMsg.MessageID)
					markSeen(pingMsg.MessageID)
					h.broadcast(pingMsg)
					relayToPeers(ctx, host, "", pingMsg)
				}
			}
		}()
	}

	mux := http.NewServeMux()

	broadcastAll := func(msg ChatMessage) {
		msg.Sender = host.ID().String()
		if msg.Nickname == "" {
			msg.Nickname = GetDefaultNickname()
		}
		msg.Hops = 0
		msg.HopTrail = []string{host.ID().String()}
		msg.Timestamp = time.Now().UnixMilli()
		nodeSeen.markSeen(msg.MessageID)
		markSeen(msg.MessageID)
		_ = saveMessage(msg)
		h.broadcast(msg)
		relayToPeers(ctx, host, "", msg)
		if store := currentAdminStore(); store != nil {
			store.record(msg)
		}
	}

	registerAdminRoutes(mux, broadcastAll, func() *bolt.DB {
		dbMu.RLock()
		defer dbMu.RUnlock()
		return db
	})

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
			ID        string   `json:"id"`
			Addrs     []string `json:"addrs"`
			LatencyMs int64    `json:"latencyMs"`
		}
		peerList := make([]peerInfo, 0)
		for _, p := range host.Network().Peers() {
			addrStrs := make([]string, 0)
			for _, conn := range host.Network().ConnsToPeer(p) {
				addrStrs = append(addrStrs, conn.RemoteMultiaddr().String())
			}
			latMs := host.Peerstore().LatencyEWMA(p).Milliseconds()
			peerList = append(peerList, peerInfo{ID: p.String(), Addrs: addrStrs, LatencyMs: latMs})
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"self":  host.ID().String(),
			"peers": peerList,
		})
	})

	// GET /groups: returns all known group channels
	mux.HandleFunc("/groups", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"groups": knownGroups(),
		})
	})

	// GET /history: paginated message history using O(log N) cursor seek
	mux.HandleFunc("/history", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		groupID := r.URL.Query().Get("group")
		if groupID == "" {
			groupID = "general"
		}
		limit := 50
		if lStr := r.URL.Query().Get("limit"); lStr != "" {
			if parsed, err := strconv.Atoi(lStr); err == nil && parsed > 0 {
				limit = parsed
			}
		}
		var sinceMs int64 = 0
		if sStr := r.URL.Query().Get("since"); sStr != "" {
			if parsed, err := strconv.ParseInt(sStr, 10, 64); err == nil {
				sinceMs = parsed
			}
		}

		msgs, hasMore, nextSince := historySince(groupID, sinceMs, limit)
		if msgs == nil {
			msgs = []ChatMessage{}
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"group":     groupID,
			"messages":  msgs,
			"hasMore":   hasMore,
			"nextSince": nextSince,
		})
	})

	// GET /presence: live roster snapshot (nicknames and last-seen from received traffic)
	mux.HandleFunc("/presence", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"self":   host.ID().String(),
			"roster": getRosterSnapshot(),
		})
	})

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

	mux.HandleFunc("/connect", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Addr string `json:"addr"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Addr == "" {
			http.Error(w, "JSON body with 'addr' required", http.StatusBadRequest)
			return
		}
		maddr, err := multiaddr.NewMultiaddr(req.Addr)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid address: " + err.Error()})
			return
		}
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid peer address: " + err.Error()})
			return
		}
		if info.ID == host.ID() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "cannot connect to self"})
			return
		}
		host.Peerstore().ClearAddrs(info.ID)
		host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.TempAddrTTL)
		dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
		defer dialCancel()
		if err := host.Connect(dialCtx, *info); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "connected", "peer": info.ID.String()})
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

	// WS /signal: dedicated WebRTC signaling endpoint isolated from chat
	handleSignalConn := func(conn *wsConn) {
		h.add(conn)
		defer h.remove(conn)

		for {
			var msg ChatMessage
			if err := conn.ReadJSON(&msg); err != nil {
				break
			}
			if isCallSignal(msg.Type) {
				msg.Sender = host.ID().String()
				msg.Timestamp = time.Now().UnixMilli()
				if msg.To == "" {
					// Room broadcast
					connectedPeers := host.Network().Peers()
					data, err := json.Marshal(msg)
					if err == nil {
						for _, p := range connectedPeers {
							go func(target peer.ID) {
								sctx, scancel := context.WithTimeout(ctx, 5*time.Second)
								defer scancel()
								stream, err := host.NewStream(sctx, target, callProtocolID)
								if err != nil {
									return
								}
								defer stream.Close()
								if _, err := stream.Write(data); err != nil {
									stream.Reset()
								}
							}(p)
						}
					}
					continue
				}

				// 1:1 call signal
				targetID, err := peer.Decode(msg.To)
				if err != nil {
					continue
				}
				sctx, scancel := context.WithTimeout(ctx, 8*time.Second)
				_ = sendDirect(sctx, host, targetID, msg)
				scancel()
			}
		}
	}

	mux.HandleFunc("/signal", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("[LANMESH-SIGNAL] WebSocket upgrade failed:", err)
			return
		}
		defer conn.Close()
		handleSignalConn(conn)
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

			// Local keepalive heartbeat from webview to keep websocket open
			if msg.Type == "ping" {
				conn.WriteJSON(ChatMessage{Type: "pong", Timestamp: time.Now().UnixMilli()})
				continue
			}

			msg.Sender = host.ID().String()

			// Call signaling over /ws
			if isCallSignal(msg.Type) {
				msg.Timestamp = time.Now().UnixMilli()
				if msg.To == "" {
					connectedPeers := host.Network().Peers()
					data, err := json.Marshal(msg)
					if err == nil {
						for _, p := range connectedPeers {
							go func(target peer.ID) {
								sctx, scancel := context.WithTimeout(ctx, 5*time.Second)
								defer scancel()
								stream, err := host.NewStream(sctx, target, callProtocolID)
								if err != nil {
									return
								}
								defer stream.Close()
								if _, err := stream.Write(data); err != nil {
									stream.Reset()
								}
							}(p)
						}
					}
					continue
				}

				targetID, err := peer.Decode(msg.To)
				if err != nil {
					continue
				}
				sctx, scancel := context.WithTimeout(ctx, 8*time.Second)
				_ = sendDirect(sctx, host, targetID, msg)
				scancel()
				continue
			}

			if msg.GroupID == "" {
				msg.GroupID = "general"
			}

			msg.MessageID = newMessageID()
			msg.Hops = 0
			msg.HopTrail = []string{host.ID().String()}
			msg.Timestamp = time.Now().UnixMilli()

			nodeSeen.markSeen(msg.MessageID)
			markSeen(msg.MessageID)
			stats.record(msg.Hops)
			updateRoster(msg.Sender, msg.Nickname)

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

	listenPort := cfg.Port
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", listenPort),
		Handler: mux,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Println("HTTP server error:", err)
		}
	}()

	if listenPort > 0 {
		startSubnetScanner(ctx, host, listenPort)
	}

	go func() {
		<-ctx.Done()
		host.Close()
	}()

	return &Node{
		PeerID:     host.ID().String(),
		Nickname:   GetDefaultNickname(),
		IsAdmin:    IsAdminNode(),
		httpServer: srv,
		cancel:     cancel,
		ctx:        ctx,
		host:       host,
		seen:       nodeSeen,
		beacon:     beaconMgr,
	}, nil
}

func ensureDir(dir string) error {
	return dirMkdirAll(dir)
}

var (
	configuredLocalIPMu sync.RWMutex
	configuredLocalIP   string
)

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

	if cfg := GetConfiguredLocalIP(); cfg != "" {
		if parsed := net.ParseIP(cfg); parsed != nil {
			addIP(parsed, "SetLocalIP")
		}
	}

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

	if ifAddrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range ifAddrs {
			if ipnet, ok := a.(*net.IPNet); ok {
				addIP(ipnet.IP, "net.InterfaceAddrs")
			}
		}
	}

	probeTargets := []string{
		"224.0.0.1:9000",
		"255.255.255.255:9000",
		"1.1.1.1:80",
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

func filterAndExpandAddrs(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
	lanIPs := findLANIPs()

	var result []multiaddr.Multiaddr
	seen := make(map[string]bool)

	for _, a := range addrs {
		str := a.String()
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