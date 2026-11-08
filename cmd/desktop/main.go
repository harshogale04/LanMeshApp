package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"time"

	"lanmesh/core"
)

// adminSecret can be pre-baked at compile time using:
// go build -ldflags="-s -w -X main.adminSecret=pspadmin" -o lanmesh-admin.exe ./cmd/desktop
var adminSecret string

func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	_ = cmd.Start()
}

func main() {
	envSecret := os.Getenv("LANMESH_ADMIN_SECRET")
	effectiveSecret := adminSecret
	if effectiveSecret == "" {
		effectiveSecret = envSecret
	}

	defaultDataDir := "./lanmesh-data"
	defaultNickname := ""
	if hostname, err := os.Hostname(); err == nil {
		defaultNickname = hostname
	}

	defaultP2PPort := 4001
	if effectiveSecret != "" {
		defaultDataDir = "./admin-data"
		defaultNickname = "Admin-HQ"
	}

	port := flag.Int("port", 8080, "Local HTTP and WebSocket server port for web UI access")
	p2pPort := flag.Int("p2p-port", defaultP2PPort, "libp2p TCP listening port for direct wire connections")
	nickname := flag.String("nickname", defaultNickname, "Display name advertised in presence heartbeats and messages")
	dataDir := flag.String("data-dir", defaultDataDir, "Directory where BoltDB (lanmesh.db) and node key (peer.key) are stored")
	flagAdminSecret := flag.String("admin-secret", effectiveSecret, "Password to enable this node as an authoritative Admin station (or set via LANMESH_ADMIN_SECRET)")
	noBrowser := flag.Bool("no-browser", false, "Disables automatically launching the default browser on daemon startup")
	flag.Parse()

	finalSecret := *flagAdminSecret
	if finalSecret == "" {
		finalSecret = os.Getenv("LANMESH_ADMIN_SECRET")
	}

	cfg := core.NodeConfig{
		Port:        *port,
		P2PPort:     *p2pPort,
		Nickname:    *nickname,
		DataDir:     *dataDir,
		AdminSecret: finalSecret,
	}

	node, err := core.StartNodeWithConfig(cfg)
	if err != nil {
		log.Fatalf("Fatal: failed to start LANMesh node: %v", err)
	}

	fmt.Println("==================================================")
fmt.Println("LANMesh — Serverless P2P Ad-Hoc Mesh Node")
fmt.Println("==================================================")
fmt.Println("Peer ID:   ", node.PeerID)
fmt.Println("Nickname:  ", node.Nickname)

if node.IsAdmin {
    fmt.Println("Role:      AUTHORITATIVE ADMIN STATION")
    fmt.Printf("Admin UI:  http://localhost:%d/admin\n", *port)
} else {
    fmt.Println("Role:      Decentralized Standard Peer")
}

fmt.Printf("Web UI:     http://localhost:%d\n", *port)

if *p2pPort > 0 {
    fmt.Printf("P2P Wire:   tcp://0.0.0.0:%d\n", *p2pPort)
}

fmt.Println("==================================================")

if !*noBrowser {
    go func() {
        time.Sleep(500 * time.Millisecond)
        openBrowser(fmt.Sprintf("http://localhost:%d", *port))
    }()
}

select {} // block forever — desktop CLI runs indefinitely
}
