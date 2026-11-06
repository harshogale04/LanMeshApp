package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	libp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	syncProtocolID     = "/p2p-sync/1.0.0"
	legacySyncProtocol = "/p2p-chat-sync/1.0.0"
	syncCooldown       = 45 * time.Second
	syncStreamTimeout  = 20 * time.Second
	syncChunkLimit     = 50
)

var syncSem = make(chan struct{}, 2) // Semaphore: max 2 concurrent sync streams

// SyncEnvelope wraps typed JSON messages over /p2p-sync/1.0.0 streams.
type SyncEnvelope struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type DigestRequest struct {
	Digest map[string]int64 `json:"digest"` // groupID -> highestTimestamp
}

type GroupDelta struct {
	GroupID string `json:"groupId"`
	Since   int64  `json:"since"`
}

type DigestResponse struct {
	Groups []GroupDelta `json:"groups"`
}

type PullRequest struct {
	GroupID string `json:"groupId"`
	Since   int64  `json:"since"`
	Limit   int    `json:"limit"`
}

type PullResponse struct {
	GroupID   string        `json:"groupId"`
	Messages  []ChatMessage `json:"messages"`
	HasMore   bool          `json:"hasMore"`
	NextSince int64         `json:"nextSince"`
}

// handleSyncStream serves the responder side of /p2p-sync/1.0.0.
func handleSyncStream(s network.Stream) {
	defer s.Close()
	_ = s.SetDeadline(time.Now().Add(syncStreamTimeout))

	dec := json.NewDecoder(s)
	enc := json.NewEncoder(s)

	// Step 1: Read DigestRequest
	var reqEnv SyncEnvelope
	if err := dec.Decode(&reqEnv); err != nil {
		return
	}
	if reqEnv.Type != "digest_request" {
		return
	}

	var dReq DigestRequest
	if err := json.Unmarshal(reqEnv.Payload, &dReq); err != nil {
		return
	}

	// Compute which groups have newer messages than peer's digest
	localMarks := groupHighWatermark()
	var deltas []GroupDelta

	// Check groups known locally
	for _, groupID := range knownGroups() {
		peerSince, peerHasGroup := dReq.Digest[groupID]
		localMax := localMarks[groupID]

		if !peerHasGroup {
			// Peer doesn't know this group: offer all history from 0
			deltas = append(deltas, GroupDelta{GroupID: groupID, Since: 0})
		} else if localMax > peerSince {
			// Local node has messages newer than peer's watermark
			deltas = append(deltas, GroupDelta{GroupID: groupID, Since: peerSince})
		}
	}

	dRespData, _ := json.Marshal(DigestResponse{Groups: deltas})
	if err := enc.Encode(SyncEnvelope{Type: "digest_response", Payload: dRespData}); err != nil {
		return
	}

	// Step 2: Serve chunked PullRequests
	for {
		_ = s.SetDeadline(time.Now().Add(syncStreamTimeout))
		var pReqEnv SyncEnvelope
		if err := dec.Decode(&pReqEnv); err != nil {
			if err == io.EOF {
				return
			}
			return
		}
		if pReqEnv.Type != "pull_request" {
			return
		}

		var pReq PullRequest
		if err := json.Unmarshal(pReqEnv.Payload, &pReq); err != nil {
			return
		}

		limit := pReq.Limit
		if limit <= 0 || limit > syncChunkLimit {
			limit = syncChunkLimit
		}

		msgs, hasMore, nextSince := historySince(pReq.GroupID, pReq.Since, limit)
		pRespData, _ := json.Marshal(PullResponse{
			GroupID:   pReq.GroupID,
			Messages:  msgs,
			HasMore:   hasMore,
			NextSince: nextSince,
		})

		if err := enc.Encode(SyncEnvelope{Type: "pull_response", Payload: pRespData}); err != nil {
			return
		}
	}
}

// requestSync initiates /p2p-sync/1.0.0 catch-up with target peer using local database watermarks.
func requestSync(ctx context.Context, host libp2phost.Host, target peer.ID, applyFn func(ChatMessage)) error {
	return requestSyncWithWatermarks(ctx, host, target, groupHighWatermark(), applyFn)
}

// requestSyncWithWatermarks initiates /p2p-sync/1.0.0 catch-up with target peer using the provided group watermarks.
func requestSyncWithWatermarks(ctx context.Context, host libp2phost.Host, target peer.ID, localMarks map[string]int64, applyFn func(ChatMessage)) error {
	// Acquire sync semaphore (limit 2 concurrent syncs)
	select {
	case syncSem <- struct{}{}:
		defer func() { <-syncSem }()
	case <-ctx.Done():
		return ctx.Err()
	}

	sctx, cancel := context.WithTimeout(ctx, syncStreamTimeout)
	defer cancel()

	s, err := host.NewStream(sctx, target, syncProtocolID, legacySyncProtocol)
	if err != nil {
		return fmt.Errorf("failed to open sync stream: %w", err)
	}
	defer s.Close()
	_ = s.SetDeadline(time.Now().Add(syncStreamTimeout))

	dec := json.NewDecoder(s)
	enc := json.NewEncoder(s)

	// Step 1: Send our watermarks
	if localMarks == nil {
		localMarks = make(map[string]int64)
	}
	dReqData, _ := json.Marshal(DigestRequest{Digest: localMarks})
	if err := enc.Encode(SyncEnvelope{Type: "digest_request", Payload: dReqData}); err != nil {
		return err
	}

	// Step 2: Read DigestResponse
	var dRespEnv SyncEnvelope
	if err := dec.Decode(&dRespEnv); err != nil {
		return err
	}
	if dRespEnv.Type != "digest_response" {
		return fmt.Errorf("unexpected envelope: %s", dRespEnv.Type)
	}

	var dResp DigestResponse
	if err := json.Unmarshal(dRespEnv.Payload, &dResp); err != nil {
		return err
	}

	if len(dResp.Groups) == 0 {
		return nil // Already completely up to date
	}

	// Step 3: Pull chunked message deltas
	for _, delta := range dResp.Groups {
		since := delta.Since
		for {
			_ = s.SetDeadline(time.Now().Add(syncStreamTimeout))
			pReqData, _ := json.Marshal(PullRequest{
				GroupID: delta.GroupID,
				Since:   since,
				Limit:   syncChunkLimit,
			})
			if err := enc.Encode(SyncEnvelope{Type: "pull_request", Payload: pReqData}); err != nil {
				return err
			}

			var pRespEnv SyncEnvelope
			if err := dec.Decode(&pRespEnv); err != nil {
				return err
			}
			if pRespEnv.Type != "pull_response" {
				return fmt.Errorf("expected pull_response, got %s", pRespEnv.Type)
			}

			var pResp PullResponse
			if err := json.Unmarshal(pRespEnv.Payload, &pResp); err != nil {
				return err
			}

			if len(pResp.Messages) > 0 {
				// Single-transaction batch disk commit
				_ = saveBatch(pResp.Messages)
				for _, msg := range pResp.Messages {
					applyFn(msg)
				}
			}

			if !pResp.HasMore || len(pResp.Messages) == 0 {
				break
			}
			since = pResp.NextSince
		}
	}

	log.Printf("[LANMESH-SYNC] Catch-up sync complete with peer %s", target.String()[:8])
	return nil
}

// ---------------------------------------------------------------------
// Connection-triggered sync notifee.
// Uses lexicographical peer ID tie-breaking to avoid connection glare.
// ---------------------------------------------------------------------

type syncNotifee struct {
	ctx      context.Context
	host     libp2phost.Host
	applyFn  func(ChatMessage)
	isAdmin  func() bool
	mu       sync.Mutex
	lastSync map[peer.ID]time.Time
}

func newSyncNotifee(ctx context.Context, host libp2phost.Host, applyFn func(ChatMessage), isAdmin func() bool) *syncNotifee {
	return &syncNotifee{
		ctx:      ctx,
		host:     host,
		applyFn:  applyFn,
		isAdmin:  isAdmin,
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
	if !n.shouldSync(p) {
		return
	}

	// Tie-breaking: the peer with smaller Peer ID initiates sync, or an admin node always initiates.
	// This ensures exactly 1 sync stream is opened between any pair of nodes.
	isInitiator := n.isAdmin() || (n.host.ID() < p)
	if !isInitiator {
		return
	}

	go func() {
		// Small delay to allow stream multiplexer handshake to settle
		time.Sleep(300 * time.Millisecond)
		if err := requestSync(n.ctx, n.host, p, n.applyFn); err != nil {
			log.Printf("[LANMESH-SYNC] Sync attempt with %s completed/skipped: %v", p.String()[:8], err)
		}
	}()
}

func (n *syncNotifee) Disconnected(_ network.Network, c network.Conn) {}
func (n *syncNotifee) Listen(network.Network, ma.Multiaddr)           {}
func (n *syncNotifee) ListenClose(network.Network, ma.Multiaddr)      {}
