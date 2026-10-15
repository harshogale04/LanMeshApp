package core

// Peer Exchange (PEX) — gossip-style peer discovery over direct connections.
//
// Problem: Android hotspot/SoftAP typically does NOT forward mDNS multicast
// between Wi-Fi clients. Device A and Device B can each discover the hotspot
// host (Phone C), but A cannot receive B's mDNS announcements via multicast
// because the AP driver silently drops client-to-client multicast frames.
//
// Solution: Once any two devices share a TCP connection (e.g. via QR-code),
// they exchange their known peers' multiaddresses over a lightweight /lanmesh-pex/1.0.0
// stream. Each side then attempts to dial any peers it didn't already know.
// This propagates peer knowledge transitively across the mesh:
//   Phone3 --QR--> Phone1 --PEX--> learns Phone2 --dials--> Phone2 connected.
//
// PEX fires on every new connection, bidirectionally. Both sides open a PEX
// stream to the other simultaneously, so the exchange is always symmetric.
// A short cooldown (30 s) prevents flooding.

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	libp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	ma "github.com/multiformats/go-multiaddr"
)

const pexProtocolID = "/lanmesh-pex/1.0.0"
const pexCooldown = 30 * time.Second
const pexTimeout = 10 * time.Second

// pexRecord is a single peer's known multiaddresses, JSON-serialised over the
// PEX stream.
type pexRecord struct {
	ID    string   `json:"id"`
	Addrs []string `json:"addrs"`
}

// pexMessage is the full payload exchanged: our own ID + addrs, plus all
// connected peers' addrs so the recipient can learn transitive peers.
type pexMessage struct {
	Peers []pexRecord `json:"peers"`
}

// pexNotifee hooks into libp2p's network notifications. On every new
// connection (either direction) it sends a PEX message to the remote peer.
type pexNotifee struct {
	ctx  context.Context
	host libp2phost.Host

	mu       sync.Mutex
	lastSent map[peer.ID]time.Time
}

func newPexNotifee(ctx context.Context, host libp2phost.Host) *pexNotifee {
	return &pexNotifee{
		ctx:      ctx,
		host:     host,
		lastSent: make(map[peer.ID]time.Time),
	}
}

// shouldSend returns true and records the attempt if pexCooldown has elapsed
// since the last PEX exchange with p.
func (n *pexNotifee) shouldSend(p peer.ID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if t, ok := n.lastSent[p]; ok && time.Since(t) < pexCooldown {
		return false
	}
	n.lastSent[p] = time.Now()
	return true
}

// Connected fires on both sides whenever a new TCP connection is established.
// We open a PEX stream to the new peer and push our known-peer table.
func (n *pexNotifee) Connected(_ network.Network, c network.Conn) {
	p := c.RemotePeer()
	if !n.shouldSend(p) {
		return
	}
	go n.sendPEX(p)
}

func (n *pexNotifee) Disconnected(_ network.Network, _ network.Conn) {}
func (n *pexNotifee) Listen(_ network.Network, _ ma.Multiaddr)        {}
func (n *pexNotifee) ListenClose(_ network.Network, _ ma.Multiaddr)   {}

// buildPEXMessage collects all currently-connected peers' multiaddresses from
// the peerstore plus this node's own advertised addresses.
func (n *pexNotifee) buildPEXMessage() pexMessage {
	msg := pexMessage{}

	// Include our own advertised addresses.
	selfAddrs := n.host.Addrs()
	if len(selfAddrs) > 0 {
		rec := pexRecord{ID: n.host.ID().String()}
		for _, a := range selfAddrs {
			rec.Addrs = append(rec.Addrs, a.String())
		}
		msg.Peers = append(msg.Peers, rec)
	}

	// Include all currently-connected peers.
	for _, p := range n.host.Network().Peers() {
		var addrStrs []string
		// Prefer the live connection addresses (definitely reachable right now).
		for _, conn := range n.host.Network().ConnsToPeer(p) {
			addrStr := conn.RemoteMultiaddr().String()
			addrStrs = append(addrStrs, addrStr)
		}
		// Also add peerstore addresses as fallback.
		for _, a := range n.host.Peerstore().Addrs(p) {
			addrStrs = append(addrStrs, a.String())
		}
		if len(addrStrs) == 0 {
			continue
		}
		// Deduplicate
		seen := make(map[string]bool)
		var dedupAddrs []string
		for _, s := range addrStrs {
			if !seen[s] {
				seen[s] = true
				dedupAddrs = append(dedupAddrs, s)
			}
		}
		msg.Peers = append(msg.Peers, pexRecord{ID: p.String(), Addrs: dedupAddrs})
	}
	return msg
}

// sendPEX opens a PEX stream to p, writes our peer table, reads theirs, then
// dials any peers we didn't know before.
func (n *pexNotifee) sendPEX(p peer.ID) {
	dialCtx, cancel := context.WithTimeout(n.ctx, pexTimeout)
	defer cancel()

	stream, err := n.host.NewStream(dialCtx, p, pexProtocolID)
	if err != nil {
		// Peer may not support PEX yet (older client) — not fatal.
		log.Printf("[PEX] Could not open PEX stream to %s: %v", p.String()[:8], err)
		return
	}
	defer stream.Close()

	enc := json.NewEncoder(stream)
	dec := json.NewDecoder(stream)

	// Send our peer table first.
	myMsg := n.buildPEXMessage()
	if err := enc.Encode(myMsg); err != nil {
		log.Printf("[PEX] Send error to %s: %v", p.String()[:8], err)
		return
	}
	log.Printf("[PEX] Sent %d peer records to %s", len(myMsg.Peers), p.String()[:8])

	// Read their peer table.
	var theirMsg pexMessage
	stream.SetDeadline(time.Now().Add(pexTimeout))
	if err := dec.Decode(&theirMsg); err != nil {
		log.Printf("[PEX] Recv error from %s: %v", p.String()[:8], err)
		return
	}
	log.Printf("[PEX] Received %d peer records from %s", len(theirMsg.Peers), p.String()[:8])

	// Dial any new peers we learned.
	n.applyPEX(theirMsg)
}

// handlePEXStream is the server side: read remote's peer table, reply with
// ours, then apply theirs.
func (n *pexNotifee) handlePEXStream(s network.Stream) {
	defer s.Close()
	remote := s.Conn().RemotePeer()

	dec := json.NewDecoder(s)
	enc := json.NewEncoder(s)

	// Read their peer table.
	var theirMsg pexMessage
	s.SetDeadline(time.Now().Add(pexTimeout))
	if err := dec.Decode(&theirMsg); err != nil {
		log.Printf("[PEX] Server: read error from %s: %v", remote.String()[:8], err)
		return
	}
	log.Printf("[PEX] Server: received %d peer records from %s", len(theirMsg.Peers), remote.String()[:8])

	// Reply with ours.
	myMsg := n.buildPEXMessage()
	s.SetDeadline(time.Now().Add(pexTimeout))
	if err := enc.Encode(myMsg); err != nil {
		log.Printf("[PEX] Server: send error to %s: %v", remote.String()[:8], err)
		return
	}

	// Apply what we learned.
	n.applyPEX(theirMsg)
}

// applyPEX processes incoming peer records and attempts to connect to any
// previously unknown or unconnected peers.
func (n *pexNotifee) applyPEX(msg pexMessage) {
	myID := n.host.ID()
	for _, rec := range msg.Peers {
		pid, err := peer.Decode(rec.ID)
		if err != nil {
			log.Printf("[PEX] Invalid peer ID %q: %v", rec.ID, err)
			continue
		}
		// Never dial ourselves.
		if pid == myID {
			continue
		}
		// Already connected — nothing to do.
		if n.host.Network().Connectedness(pid) == network.Connected {
			continue
		}

		// Parse and filter addresses.
		var dialAddrs []ma.Multiaddr
		for _, addrStr := range rec.Addrs {
			a, err := ma.NewMultiaddr(addrStr)
			if err != nil {
				continue
			}
			s := a.String()
			if isLoopbackAddr(s) {
				continue
			}
			dialAddrs = append(dialAddrs, a)
		}
		if len(dialAddrs) == 0 {
			log.Printf("[PEX] Peer %s has no dialable addrs from PEX — skipping", pid.String()[:8])
			continue
		}

		// Register addresses and dial.
		n.host.Peerstore().AddAddrs(pid, dialAddrs, peerstore.TempAddrTTL)
		info := peer.AddrInfo{ID: pid, Addrs: dialAddrs}
		go func(ai peer.AddrInfo) {
			ctx, cancel := context.WithTimeout(n.ctx, 8*time.Second)
			defer cancel()
			log.Printf("[PEX] Attempting dial to PEX-learned peer %s at %v", ai.ID.String()[:8], ai.Addrs)
			if err := n.host.Connect(ctx, ai); err != nil {
				log.Printf("[PEX] Dial to %s failed: %v", ai.ID.String()[:8], err)
			} else {
				log.Printf("[PEX] Connected to PEX-learned peer %s", ai.ID.String()[:8])
			}
		}(info)
	}
}

// isLoopbackAddr returns true for addresses containing loopback or wildcard IPs.
func isLoopbackAddr(s string) bool {
	return strings.Contains(s, "/127.0.0.1/") ||
		strings.Contains(s, "/::1/") ||
		strings.Contains(s, "/0.0.0.0/")
}

// setupPEX registers the PEX stream handler and connection notifee on host.
func setupPEX(ctx context.Context, host libp2phost.Host) {
	notifee := newPexNotifee(ctx, host)

	// Register server-side handler for incoming PEX streams.
	host.SetStreamHandler(pexProtocolID, notifee.handlePEXStream)

	// Register connection notifee: fires sendPEX whenever we connect to a new peer.
	host.Network().Notify(notifee)

	log.Printf("[PEX] Peer exchange protocol registered (%s)", pexProtocolID)
}
