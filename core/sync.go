package core

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	libp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

const syncProtocolID = "/p2p-sync/1.0.0"

// syncSem limits simultaneous active sync streams to 2, preventing CPU,
// disk, and Wi-Fi interface saturation.
var syncSem = make(chan struct{}, 2)

// syncCooldown enforces a 45-second debounce window per peer,
// preventing connection-flapping sync storms.
const syncCooldown = 45 * time.Second

type syncGroupManifest struct {
	GroupID    string   `json:"groupId"`
	MessageIDs []string `json:"messageIds"`
}

type syncManifest struct {
	Groups []syncGroupManifest `json:"groups"`
}

type syncPayload struct {
	Missing  []ChatMessage       `json:"missing"`
	Manifest []syncGroupManifest `json:"manifest,omitempty"`
}

func messageIndexFor(groupID string) map[string]ChatMessage {
	index := make(map[string]ChatMessage)
	for _, m := range historyFor(groupID) {
		if m.MessageID != "" {
			index[m.MessageID] = m
		}
	}
	return index
}

func groupKnownLocally(groupID string) bool {
	for _, g := range knownGroups() {
		if g == groupID {
			return true
		}
	}
	return false
}

func buildLocalManifest() []syncGroupManifest {
	var manifest []syncGroupManifest
	for _, g := range knownGroups() {
		index := messageIndexFor(g)
		ids := make([]string, 0, len(index))
		for id := range index {
			ids = append(ids, id)
		}
		manifest = append(manifest, syncGroupManifest{GroupID: g, MessageIDs: ids})
	}
	return manifest
}

func computeMissing(theirManifest []syncGroupManifest) []ChatMessage {
	haveByGroup := make(map[string]map[string]bool)
	for _, g := range theirManifest {
		set := make(map[string]bool, len(g.MessageIDs))
		for _, id := range g.MessageIDs {
			set[id] = true
		}
		haveByGroup[g.GroupID] = set
	}

	var missing []ChatMessage
	for _, groupID := range knownGroups() {
		theyHave := haveByGroup[groupID]
		for _, m := range historyFor(groupID) {
			if m.MessageID == "" {
				continue
			}
			if theyHave == nil || !theyHave[m.MessageID] {
				missing = append(missing, m)
			}
		}
	}
	return missing
}

func syncServer(s network.Stream, applyFn func(ChatMessage)) {
	defer s.Close()

	select {
	case syncSem <- struct{}{}:
		defer func() { <-syncSem }()
	default:
		return
	}

	dec := json.NewDecoder(s)
	enc := json.NewEncoder(s)

	// Step 1: read their manifest
	var reqManifest syncManifest
	if err := dec.Decode(&reqManifest); err != nil {
		return
	}

	// Step 2: reply with what they are missing + our manifest
	reply := syncPayload{
		Missing:  computeMissing(reqManifest.Groups),
		Manifest: buildLocalManifest(),
	}
	if err := enc.Encode(reply); err != nil {
		return
	}

	// Step 3: read back what we are missing, apply it
	var final syncPayload
	if err := dec.Decode(&final); err != nil {
		return
	}
	for _, m := range final.Missing {
		applyFn(m)
	}
}

func requestSync(ctx context.Context, host libp2phost.Host, p peer.ID, applyFn func(ChatMessage)) {
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	stream, err := host.NewStream(sctx, p, syncProtocolID)
	if err != nil {
		return
	}
	defer stream.Close()

	dec := json.NewDecoder(stream)
	enc := json.NewEncoder(stream)

	// Step 1: send our manifest
	if err := enc.Encode(syncManifest{Groups: buildLocalManifest()}); err != nil {
		return
	}

	// Step 2: read what we're missing + their manifest
	var reply syncPayload
	if err := dec.Decode(&reply); err != nil {
		return
	}
	for _, m := range reply.Missing {
		applyFn(m)
	}

	// Step 3: send what they are missing
	final := syncPayload{Missing: computeMissing(reply.Manifest)}
	if err := enc.Encode(final); err != nil {
		return
	}
}

type syncNotifee struct {
	ctx     context.Context
	host    libp2phost.Host
	applyFn func(ChatMessage)

	mu       sync.Mutex
	lastSync map[peer.ID]time.Time
}

func newSyncNotifee(ctx context.Context, host libp2phost.Host, applyFn func(ChatMessage)) *syncNotifee {
	return &syncNotifee{
		ctx:      ctx,
		host:     host,
		applyFn:  applyFn,
		lastSync: make(map[peer.ID]time.Time),
	}
}

func (n *syncNotifee) shouldSync(p peer.ID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if t, ok := n.lastSync[p]; ok && time.Since(t) < syncCooldown {
		return false
	}
	n.lastSync[p] = time.Now()
	return true
}

func (n *syncNotifee) Connected(_ network.Network, c network.Conn) {
	p := c.RemotePeer()
	// Glare Prevention: Lexicographical Peer ID tie-breaking (host.ID() < remoteID)
	// so only one side initiates sync per peer pair
	if n.host.ID() >= p {
		return
	}
	if !n.shouldSync(p) {
		return
	}
	go func() {
		select {
		case syncSem <- struct{}{}:
			defer func() { <-syncSem }()
		case <-n.ctx.Done():
			return
		}
		requestSync(n.ctx, n.host, p, n.applyFn)
	}()
}

func (n *syncNotifee) Disconnected(_ network.Network, c network.Conn) {}
func (n *syncNotifee) Listen(network.Network, ma.Multiaddr)           {}
func (n *syncNotifee) ListenClose(network.Network, ma.Multiaddr)      {}
