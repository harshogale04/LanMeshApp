package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// SubnetScanner performs automatic subnet discovery over TCP/HTTP.
//
// Motivation: On many Android hotspots (and some enterprise/campus Wi-Fi),
// multicast packets (such as mDNS at 224.0.0.251:5353) are blocked or isolated
// between Wi-Fi client stations. As a result, three phones connected to the same
// hotspot can communicate over unicast IP/TCP, but cannot discover each other via mDNS.
//
// SubnetScanner solves cold-start discovery by:
// 1. Determining the local device's IPv4 address and /24 subnet (e.g. 192.168.43.0/24).
// 2. Probing all host IPs on the subnet (1..254) on the LANMESH HTTP port (e.g. 9000).
// 3. Querying GET http://<ip>:9000/debug/addrs to retrieve the remote peer's advertised
//    libp2p multiaddresses and peer ID.
// 4. Dialing the discovered peer via libp2p. Once even one connection is made,
//    the Peer Exchange (PEX) protocol transitively connects all remaining nodes.
type SubnetScanner struct {
	ctx      context.Context
	cancel   context.CancelFunc
	host     host.Host
	httpPort int

	mu       sync.Mutex
	scanning bool
}

func startSubnetScanner(ctx context.Context, h host.Host, httpPort int) *SubnetScanner {
	scanCtx, cancel := context.WithCancel(ctx)
	s := &SubnetScanner{
		ctx:      scanCtx,
		cancel:   cancel,
		host:     h,
		httpPort: httpPort,
	}

	go s.runLoop()
	return s
}

func (s *SubnetScanner) runLoop() {
	// Initial delay: wait 3 seconds after startup so mDNS has a chance first
	select {
	case <-s.ctx.Done():
		return
	case <-time.After(3 * time.Second):
	}

	// Trigger immediate scan on startup
	s.scanOnce()

	// Periodic scans: repeat every 15 seconds if fewer than 2 peers are connected
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.scanOnce()
		}
	}
}

// scanOnce performs one pass across the local /24 subnet.
func (s *SubnetScanner) scanOnce() {
	s.mu.Lock()
	if s.scanning {
		s.mu.Unlock()
		return
	}
	s.scanning = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.scanning = false
		s.mu.Unlock()
	}()

	lanIPs := findLANIPs()
	if len(lanIPs) == 0 {
		return
	}

	client := &http.Client{
		Timeout: 400 * time.Millisecond, // very short timeout for fast subnet sweep
	}

	for _, localIP := range lanIPs {
		ipv4 := localIP.To4()
		if ipv4 == nil || ipv4.IsLoopback() {
			continue
		}

		// Only scan standard private IPv4 ranges (192.168.x.x, 10.x.x.x, 172.16-31.x.x)
		if !isPrivateIPv4(ipv4) {
			continue
		}

		// Calculate /24 prefix: a.b.c.
		prefix := fmt.Sprintf("%d.%d.%d.", ipv4[0], ipv4[1], ipv4[2])
		localLastOctet := int(ipv4[3])

		// Concurrently probe all other 253 host IPs in the /24 subnet
		var wg sync.WaitGroup
		sem := make(chan struct{}, 32) // throttle to 32 concurrent probes

		for i := 1; i <= 254; i++ {
			if i == localLastOctet {
				continue
			}

			targetIP := fmt.Sprintf("%s%d", prefix, i)
			wg.Add(1)
			sem <- struct{}{}

			go func(ip string) {
				defer wg.Done()
				defer func() { <-sem }()

				s.probeIP(client, ip)
			}(targetIP)
		}

		wg.Wait()
	}
}

// probeIP checks if a LANMESH node is running on targetIP:httpPort.
func (s *SubnetScanner) probeIP(client *http.Client, ip string) {
	select {
	case <-s.ctx.Done():
		return
	default:
	}

	url := fmt.Sprintf("http://%s:%d/debug/addrs", ip, s.httpPort)
	req, err := http.NewRequestWithContext(s.ctx, "GET", url, nil)
	if err != nil {
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
	if err != nil {
		return
	}

	// Parse {"self": "12D3Koo...", "addrs": ["/ip4/192.168.43.10/tcp/45343/p2p/12D3Koo..."]}
	var payload struct {
		Self  string   `json:"self"`
		Addrs []string `json:"addrs"`
	}
	if err := jsonUnmarshal(body, &payload); err != nil {
		return
	}

	if payload.Self == "" || payload.Self == s.host.ID().String() {
		return
	}

	remotePID, err := peer.Decode(payload.Self)
	if err != nil {
		return
	}

	// If already connected, skip dialing
	if s.host.Network().Connectedness(remotePID) == network.Connected {
		return
	}

	// Extract dialable addresses
	var dialAddrs []multiaddr.Multiaddr
	for _, aStr := range payload.Addrs {
		m, err := multiaddr.NewMultiaddr(aStr)
		if err != nil {
			continue
		}
		sStr := m.String()
		if strings.Contains(sStr, "/127.0.0.1/") || strings.Contains(sStr, "/0.0.0.0/") || strings.Contains(sStr, "/::1/") {
			continue
		}
		// Strip /p2p/ suffix if present to get clean transport multiaddr
		info, err := peer.AddrInfoFromP2pAddr(m)
		if err == nil {
			dialAddrs = append(dialAddrs, info.Addrs...)
		} else {
			dialAddrs = append(dialAddrs, m)
		}
	}

	if len(dialAddrs) == 0 {
		return
	}

	cleanInfo := peer.AddrInfo{
		ID:    remotePID,
		Addrs: dialAddrs,
	}

	// Connect to discovered peer
	dialCtx, dialCancel := context.WithTimeout(s.ctx, 6*time.Second)
	defer dialCancel()

	log.Printf("[SubnetScanner] Discovered LANMESH node on %s (peer %s, addrs %v) — initiating auto-connect...", ip, remotePID.String()[:8], dialAddrs)
	if err := s.host.Connect(dialCtx, cleanInfo); err != nil {
		log.Printf("[SubnetScanner] Auto-connect to %s failed: %v", remotePID.String()[:8], err)
	} else {
		log.Printf("[SubnetScanner] Successfully auto-connected to %s via subnet scan!", remotePID.String()[:8])
	}
}

func isPrivateIPv4(ip net.IP) bool {
	if ip == nil {
		return false
	}
	// 10.0.0.0/8
	if ip[0] == 10 {
		return true
	}
	// 172.16.0.0/12
	if ip[0] == 172 && ip[1] >= 16 && ip[1] <= 31 {
		return true
	}
	// 192.168.0.0/16
	if ip[0] == 192 && ip[1] == 168 {
		return true
	}
	return false
}

// jsonUnmarshal helper to avoid json import conflict
var jsonUnmarshal = func(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
