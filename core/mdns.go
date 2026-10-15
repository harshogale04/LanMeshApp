package core

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/multiformats/go-multiaddr"
)

const mdnsServiceName = "p2p-voice-mdns"

type discoveryNotifee struct {
	ctx          context.Context
	h            host.Host
	mu           sync.Mutex
	dialing      map[peer.ID]time.Time
	lastAttempt  map[peer.ID]time.Time
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if pi.ID == "" || pi.ID == n.h.ID() {
		return
	}

	// Filter out unroutable / loopback addresses and log each rejection
	var dialableAddrs []multiaddr.Multiaddr
	for _, a := range pi.Addrs {
		s := a.String()
		switch {
		case strings.Contains(s, "/0.0.0.0/"):
			log.Printf("[discovery] Skipping wildcard addr from peer %s: %s", pi.ID.String()[:8], s)
		case strings.Contains(s, "/127.0.0.1/"):
			log.Printf("[discovery] Skipping loopback addr from peer %s: %s", pi.ID.String()[:8], s)
		case strings.Contains(s, "/::1/"):
			log.Printf("[discovery] Skipping IPv6-loopback addr from peer %s: %s", pi.ID.String()[:8], s)
		default:
			dialableAddrs = append(dialableAddrs, a)
		}
	}

	if len(dialableAddrs) == 0 {
		log.Printf("[discovery] Peer %s has NO dialable addresses (all %d rejected). Remote filterAndExpandAddrs may have failed.", pi.ID.String()[:8], len(pi.Addrs))
		return
	}

	cleanInfo := peer.AddrInfo{
		ID:    pi.ID,
		Addrs: dialableAddrs,
	}

	n.mu.Lock()
	// Skip if currently connected
	if n.h.Network().Connectedness(pi.ID) == network.Connected {
		n.mu.Unlock()
		return
	}

	// Check if already dialing or dialed very recently (backoff: 10s)
	if inProgressSince, ok := n.dialing[pi.ID]; ok && time.Since(inProgressSince) < 15*time.Second {
		n.mu.Unlock()
		return
	}
	if lastAt, ok := n.lastAttempt[pi.ID]; ok && time.Since(lastAt) < 10*time.Second {
		n.mu.Unlock()
		return
	}

	now := time.Now()
	n.dialing[pi.ID] = now
	n.lastAttempt[pi.ID] = now
	n.mu.Unlock()

	log.Printf("[discovery] Peer discovered: %s (addrs: %v). Initiating dial...", pi.ID.String()[:8], dialableAddrs)

	go func() {
		defer func() {
			n.mu.Lock()
			delete(n.dialing, pi.ID)
			n.mu.Unlock()
		}()

		dialCtx, cancel := context.WithTimeout(n.ctx, 8*time.Second)
		defer cancel()

		// Purge stale/dead cached addresses from libp2p peerstore before dialing
		// to prevent dialing obsolete ports/IPs from previous sessions.
		n.h.Peerstore().ClearAddrs(pi.ID)
		n.h.Peerstore().AddAddrs(cleanInfo.ID, cleanInfo.Addrs, peerstore.TempAddrTTL)

		err := n.h.Connect(dialCtx, cleanInfo)
		if err != nil {
			log.Printf("[discovery] Dial failed to %s: %v", pi.ID.String()[:8], err)
		} else {
			log.Printf("[discovery] Successfully connected to peer %s", pi.ID.String()[:8])
			n.mu.Lock()
			delete(n.lastAttempt, pi.ID)
			n.mu.Unlock()
		}
	}()
}

func setupMDNS(ctx context.Context, h host.Host) error {
	notifee := &discoveryNotifee{
		ctx:         ctx,
		h:           h,
		dialing:     make(map[peer.ID]time.Time),
		lastAttempt: make(map[peer.ID]time.Time),
	}
	service := mdns.NewMdnsService(h, mdnsServiceName, notifee)
	return service.Start()
}