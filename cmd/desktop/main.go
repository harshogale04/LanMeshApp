package main

import (
	"flag"
	"fmt"
	"log"

	"lanmesh/core" // adjust to your actual module path from go.mod
)

func main() {
	port := flag.Int("port", 8080, "HTTP/WebSocket port to listen on")
	dataDir := flag.String("data-dir", ".", "directory for this node's bbolt DB and admin log")
	flag.Parse()

	node, err := core.StartNode(*port, *dataDir)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Your Peer ID:", node.PeerID)
	fmt.Printf("Server running at http://localhost:%d\n", *port)

	select {} // block forever — desktop CLI never returns on its own
}