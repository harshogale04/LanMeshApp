package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"lanmesh/core"
)

// adminSecret can be injected at compile time via -ldflags="-X main.adminSecret=YOUR_PASSWORD"
var adminSecret string

func main() {
	defaultDataDir := "./lanmesh-data"
	defaultNickname := ""
	if adminSecret != "" {
		defaultDataDir = "./admin-data"
		defaultNickname = "Admin-HQ"
	}

	port := flag.Int("port", 8080, "HTTP and WebSocket server port for web UI access")
	p2pPort := flag.Int("p2p-port", 4001, "libp2p TCP listening port for direct wire connections")
	nickname := flag.String("nickname", defaultNickname, "display name advertised in presence heartbeats")
	dataDir := flag.String("data-dir", defaultDataDir, "directory where BoltDB database and node key are stored")
	secretFlag := flag.String("admin-secret", adminSecret, "password to enable this node as an authoritative Admin station")
	noBrowser := flag.Bool("no-browser", false, "disables automatically launching the default browser on daemon startup")
	flag.Parse()

	activeSecret := *secretFlag
	if activeSecret == "" {
		activeSecret = os.Getenv("LANMESH_ADMIN_SECRET")
	}

	opts := core.NodeOptions{
		Port:        *port,
		P2PPort:     *p2pPort,
		Nickname:    *nickname,
		DataDir:     *dataDir,
		AdminSecret: activeSecret,
		NoBrowser:   *noBrowser,
	}

	node, err := core.StartNodeWithOptions(opts)
	if err != nil {
		log.Fatalf("Fatal: failed to start LANMesh node: %v", err)
	}

	fmt.Println("==================================================")
	fmt.Println("LANMesh — Serverless P2P Ad-Hoc Mesh Node")
	fmt.Println("==================================================")
	fmt.Println("Peer ID:   ", node.PeerID)
	fmt.Printf("Web UI:     http://localhost:%d\n", *port)
	if *p2pPort > 0 {
		fmt.Printf("P2P Wire:   tcp://0.0.0.0:%d\n", *p2pPort)
	}
	if activeSecret != "" {
		fmt.Printf("Admin Mode: http://localhost:%d/admin (Authoritative Audit Active)\n", *port)
	}
	fmt.Println("==================================================")

	select {} // block forever — desktop CLI runs indefinitely
}
