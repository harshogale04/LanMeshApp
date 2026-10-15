package mobile

import (
	"errors"
	"sync"
	"strings"

	"lanmesh/core"
)

var (
	mu      sync.Mutex
	current *core.Node
)

// Start brings up a LANMESH node inside the Android process. dataDir
// must be an app-writable path (Android's Context.getFilesDir()) — the
// app sandbox has no "current directory" the way desktop does. Returns
// the local peer ID on success, or an error string on failure — kept as
// plain strings/ints since gomobile bind only supports simple types
// across the Go/Java boundary.
func Start(port int, dataDir string) (string, error) {
	mu.Lock()
	defer mu.Unlock()
	if current != nil {
		return "", errors.New("node already running")
	}
	node, err := core.StartNode(port, dataDir)
	if err != nil {
		return "", err
	}
	current = node
	return node.PeerID, nil
}

// Stop shuts the node down cleanly. Call this from the Android
// foreground service's onDestroy so libp2p/bbolt/the HTTP server are
// released instead of leaking when the service is killed.
func Stop() error {
	mu.Lock()
	defer mu.Unlock()
	if current == nil {
		return nil
	}
	err := current.Stop()
	current = nil
	return err
}

func IsRunning() bool {
	mu.Lock()
	defer mu.Unlock()
	return current != nil
}

func Peers() string {
	mu.Lock()
	defer mu.Unlock()
	if current == nil {
		return ""
	}
	return strings.Join(current.PeerList(), "\n")
}

// ConnectToPeer manually dials a peer by its full multiaddr, bypassing
// mDNS. Use when automatic discovery fails (e.g. network isolation).
func ConnectToPeer(addr string) error {
	mu.Lock()
	defer mu.Unlock()
	if current == nil {
		return errors.New("node not running")
	}
	return current.ConnectToPeer(addr)
}

// SetLocalIP registers the device's local LAN IPv4 address (e.g. from Android's
// ConnectivityManager). This ensures that libp2p advertises and connects using
// the real Wi-Fi/hotspot IP even if native Linux netlink queries are blocked by
// Android's SELinux policy.
func SetLocalIP(ip string) {
	core.SetLocalIP(ip)
}

// MyAddresses returns this device's own connectable addresses, one per
// line, to show the user or embed in a QR code. Returns an error if no
// routable LAN address is available.
func MyAddresses() (string, error) {
	mu.Lock()
	defer mu.Unlock()
	if current == nil {
		return "", errors.New("node not running")
	}
	addrs := current.ListenAddrsString()
	if strings.TrimSpace(addrs) == "" {
		return "", errors.New("no routable LAN address available; please ensure phone is connected to Wi-Fi or hotspot")
	}
	return addrs, nil
}