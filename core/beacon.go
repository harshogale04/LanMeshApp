package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/multiformats/go-multiaddr"
)

const (
	beaconPort          = 9999
	beaconInterval      = 2500 * time.Millisecond
	beaconDialTimeout   = 5 * time.Second
	beaconRetryCooldown = 10 * time.Second
)

// BeaconPayload represents the UDP LAN broadcast announcement frame.
type BeaconPayload struct {
	PeerID string   `json:"peerId"`
	Addrs  []string `json:"addrs"`
}

// BeaconManager handles zero-config LAN discovery using UDP broadcasts (255.255.255.255:9999).
// Solves Android 11+ SELinux restrictions that block mDNS route-table netlink inspection.
type BeaconManager struct {
	ctx         context.Context
	cancel      context.CancelFunc
	host        host.Host
	mu          sync.Mutex
	lastDial    map[peer.ID]time.Time
	dialing     map[peer.ID]bool
	listenerConn *net.UDPConn
}

// startBeaconEngine initializes and runs the UDP broadcast sender and receiver.
func startBeaconEngine(ctx context.Context, h host.Host) (*BeaconManager, error) {
	bCtx, cancel := context.WithCancel(ctx)
	bm := &BeaconManager{
		ctx:      bCtx,
		cancel:   cancel,
		host:     h,
		lastDial: make(map[peer.ID]time.Time),
		dialing:  make(map[peer.ID]bool),
	}

	// 1. Start UDP Listener on :9999
	listenAddr := &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: beaconPort,
	}

	listener, err := net.ListenUDP("udp4", listenAddr)
	if err != nil {
		log.Printf("[LANMESH-BEACON] Warning: Could not bind UDP :%d for listening: %v", beaconPort, err)
		// We still allow the sender to run even if listening failed (e.g. secondary node on same machine)
	} else {
		bm.listenerConn = listener
		go bm.listenLoop(listener)
		log.Printf("[LANMESH-BEACON] UDP listener active on :%d", beaconPort)
	}

	// 2. Start UDP Broadcaster loop
	go bm.broadcastLoop()

	return bm, nil
}

func (bm *BeaconManager) broadcastLoop() {
	ticker := time.NewTicker(beaconInterval)
	defer ticker.Stop()

	bcastAddr := &net.UDPAddr{
		IP:   net.IPv4bcast,
		Port: beaconPort,
	}

	// Outbound ephemeral UDP socket
	senderConn, err := net.ListenUDP("udp4", nil)
	if err != nil {
		log.Printf("[LANMESH-BEACON] Error creating outbound UDP socket: %v", err)
		return
	}
	defer senderConn.Close()

	for {
		select {
		case <-bm.ctx.Done():
			return
		case <-ticker.C:
			bm.sendBeacon(senderConn, bcastAddr)
		}
	}
}

func (bm *BeaconManager) sendBeacon(conn *net.UDPConn, target *net.UDPAddr) {
	var addrs []string
	for _, a := range bm.host.Addrs() {
		addrs = append(addrs, a.String())
	}
	if len(addrs) == 0 {
		return
	}

	payload := BeaconPayload{
		PeerID: bm.host.ID().String(),
		Addrs:  addrs,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return
	}

	if _, err := conn.WriteToUDP(data, target); err != nil {
		// Log rarely if broadcast encounters route issues
		return
	}
}

func (bm *BeaconManager) listenLoop(conn *net.UDPConn) {
	defer conn.Close()
	buf := make([]byte, 4096)

	for {
		select {
		case <-bm.ctx.Done():
			return
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			select {
			case <-bm.ctx.Done():
				return
			default:
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		if n <= 0 {
			continue
		}

		var payload BeaconPayload
		if err := json.Unmarshal(buf[:n], &payload); err != nil {
			continue
		}

		// Discard our own broadcast beacon
		if payload.PeerID == bm.host.ID().String() {
			continue
		}

		targetID, err := peer.Decode(payload.PeerID)
		if err != nil {
			continue
		}

		// Skip if already connected
		if bm.host.Network().Connectedness(targetID) == network.Connected {
			continue
		}

		// Throttle / debounce dialing
		bm.mu.Lock()
		if bm.dialing[targetID] {
			bm.mu.Unlock()
			continue
		}
		if time.Since(bm.lastDial[targetID]) < beaconRetryCooldown {
			bm.mu.Unlock()
			continue
		}
		bm.dialing[targetID] = true
		bm.lastDial[targetID] = time.Now()
		bm.mu.Unlock()

		// Resolve addresses and substitute 0.0.0.0 with remoteAddr.IP
		var dialAddrs []multiaddr.Multiaddr
		remoteIPStr := remoteAddr.IP.String()

		for _, addrStr := range payload.Addrs {
			// Replace wildcard with real sender IP
			if strings.Contains(addrStr, "/0.0.0.0/") {
				addrStr = strings.Replace(addrStr, "/0.0.0.0/", fmt.Sprintf("/%s/", remoteIPStr), 1)
			}
			ma, err := multiaddr.NewMultiaddr(addrStr)
			if err == nil {
				dialAddrs = append(dialAddrs, ma)
			}
		}

		if len(dialAddrs) == 0 {
			bm.mu.Lock()
			bm.dialing[targetID] = false
			bm.mu.Unlock()
			continue
		}

		go func(pid peer.ID, addrs []multiaddr.Multiaddr) {
			defer func() {
				bm.mu.Lock()
				bm.dialing[pid] = false
				bm.mu.Unlock()
			}()

			bm.host.Peerstore().AddAddrs(pid, addrs, peerstore.TempAddrTTL)
			dctx, cancel := context.WithTimeout(bm.ctx, beaconDialTimeout)
			defer cancel()

			info := peer.AddrInfo{ID: pid, Addrs: addrs}
			if err := bm.host.Connect(dctx, info); err != nil {
				// Dial failed or timed out
				return
			}
			log.Printf("[LANMESH-BEACON] Discovered and connected to peer %s via UDP beacon", pid.String()[:8])
		}(targetID, dialAddrs)
	}
}

// Stop cleanly terminates the beacon manager.
func (bm *BeaconManager) Stop() {
	bm.cancel()
	if bm.listenerConn != nil {
		bm.listenerConn.Close()
	}
}

