package core

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	libp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	bolt "go.etcd.io/bbolt"
)

type ChatMessage struct {
	Type      string   `json:"type"`
	Payload   string   `json:"payload"`
	Sender    string   `json:"sender"`
	GroupID   string   `json:"groupId"`
	MessageID string   `json:"messageId"`
	Hops      int      `json:"hops"`
	Timestamp int64    `json:"timestamp"`
	Nickname  string   `json:"nickname"`
	FileName  string   `json:"fileName,omitempty"`
	To        string   `json:"to,omitempty"`
	HopTrail  []string `json:"hopTrail,omitempty"`
}

const maxHops = 6
const protocolID = "/p2p-chat/1.0.0"

// callProtocolID carries live call signaling only (1:1 and group room calls).
// It deliberately bypasses markSeen/saveMessage/relayToPeers: signaling
// is ephemeral and point-to-point between directly connected peers,
// not persisted chat history and never multi-hop relayed — routing a
// call's media or signaling through intermediate chat nodes would add latency
// and break direct WebRTC peer connections.
const callProtocolID = "/lanmesh-call/1.0.0"

func isCallSignal(t string) bool {
	switch t {
	case "call-offer", "call-answer", "call-ice", "call-end",
		"room-call-start", "room-call-join", "room-call-leave", "room-call-presence", "room-call-end",
		"room-offer", "room-answer", "room-ice",
		"peer-identity": // Ephemeral display-name exchange — not persisted or relayed over chat mesh
		return true
	}
	return false
}

// sendDirect opens a one-shot stream straight to a single peer and
// writes msg — used for direct 1:1 call and room signaling.
func sendDirect(ctx context.Context, host libp2phost.Host, to peer.ID, msg ChatMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	stream, err := host.NewStream(ctx, to, callProtocolID)
	if err != nil {
		return err
	}
	defer stream.Close()
	_, err = stream.Write(data)
	return err
}

func newMessageID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(b))
}

// seenCache is a bounded, two-generation deduplication cache.
// It stores up to maxSize entries without sudden wipes: when the current
// generation fills up to maxSize/2, the previous generation is replaced by
// the current one, and a new current generation begins. This guarantees that
// between maxSize/2 and maxSize recent message IDs are always retained.
type seenCache struct {
	mu      sync.Mutex
	curr    map[string]bool
	prev    map[string]bool
	maxSize int
}

func newSeenCache(maxSize int) *seenCache {
	if maxSize <= 0 {
		maxSize = 5000
	}
	return &seenCache{
		curr:    make(map[string]bool),
		prev:    make(map[string]bool),
		maxSize: maxSize,
	}
}

// markSeen returns true if id was already seen or is invalid (empty string),
// and false if this is the first time id has been observed.
func (c *seenCache) markSeen(id string) bool {
	if id == "" {
		return true // treat empty ID as seen/invalid to prevent flood of blank envelopes
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.curr[id] || c.prev[id] {
		return true
	}

	c.curr[id] = true
	if len(c.curr) >= c.maxSize/2 {
		c.prev = c.curr
		c.curr = make(map[string]bool, c.maxSize/2)
	}
	return false
}

var defaultSeen = newSeenCache(5000)

func markSeen(id string) bool {
	return defaultSeen.markSeen(id)
}

var relaySem = make(chan struct{}, 32)

func relayToPeers(ctx context.Context, host libp2phost.Host, exclude peer.ID, msg ChatMessage) {
	if msg.Hops >= maxHops {
		return
	}
	msg.Hops++
	// Append current node to the hop trail
	trailCopy := make([]string, len(msg.HopTrail), len(msg.HopTrail)+1)
	copy(trailCopy, msg.HopTrail)
	trailCopy = append(trailCopy, host.ID().String())
	msg.HopTrail = trailCopy

	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	for _, p := range host.Network().Peers() {
		// Do not forward back to the peer that delivered this message,
		// and never forward back to the original author of the message.
		if p == exclude || p.String() == msg.Sender {
			continue
		}
		go func(target peer.ID) {
			sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			// Concurrency throttling: acquire relay slot or drop if saturated
			select {
			case relaySem <- struct{}{}:
				defer func() { <-relaySem }()
			case <-sctx.Done():
				return
			}

			stream, err := host.NewStream(sctx, target, protocolID)
			if err != nil {
				return
			}
			defer stream.Close()
			if _, err := stream.Write(data); err != nil {
				stream.Reset()
				return
			}
		}(p)
	}
}

var (
	dbMu sync.RWMutex
	db   *bolt.DB
)

const (
	groupsBucket   = "__groups__"
	msgIndexBucket = "msg_index"
)

func initStore(path string) error {
	dbMu.Lock()
	defer dbMu.Unlock()
	if db != nil {
		db.Close()
	}
	var err error
	db, err = bolt.Open(path, 0600, &bolt.Options{Timeout: 3 * time.Second})
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(groupsBucket)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(msgIndexBucket)); err != nil {
			return err
		}
		return nil
	})
}

func saveMessage(msg ChatMessage) error {
	if msg.GroupID == "" {
		msg.GroupID = "general"
	}
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixMilli()
	}
	dbMu.RLock()
	defer dbMu.RUnlock()
	if db == nil {
		return fmt.Errorf("db not initialized")
	}
	return db.Update(func(tx *bolt.Tx) error {
		groups, err := tx.CreateBucketIfNotExists([]byte(groupsBucket))
		if err != nil {
			return err
		}
		if err := groups.Put([]byte(msg.GroupID), []byte("1")); err != nil {
			return err
		}
		if msg.Type == "group_announce" || msg.Type == "typing" {
			return nil
		}
		bucket, err := tx.CreateBucketIfNotExists([]byte(msg.GroupID))
		if err != nil {
			return err
		}
		idxBucket, err := tx.CreateBucketIfNotExists([]byte(msgIndexBucket))
		if err != nil {
			return err
		}
		// O(1) duplicate suppression via msg_index bucket
		if msg.MessageID != "" && idxBucket.Get([]byte(msg.MessageID)) != nil {
			return nil // already exists locally
		}

		key := MakeTimestampKey(msg.Timestamp, msg.MessageID)
		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		if err := bucket.Put(key, data); err != nil {
			return err
		}
		if msg.MessageID != "" {
			return idxBucket.Put([]byte(msg.MessageID), []byte(msg.GroupID))
		}
		return nil
	})
}

// saveBatch commits up to 50 messages in a single BoltDB transaction (1 disk fsync).
func saveBatch(messages []ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}
	dbMu.RLock()
	defer dbMu.RUnlock()
	if db == nil {
		return fmt.Errorf("db not initialized")
	}
	return db.Update(func(tx *bolt.Tx) error {
		groups, err := tx.CreateBucketIfNotExists([]byte(groupsBucket))
		if err != nil {
			return err
		}
		idxBucket, err := tx.CreateBucketIfNotExists([]byte(msgIndexBucket))
		if err != nil {
			return err
		}

		for _, msg := range messages {
			if msg.GroupID == "" {
				msg.GroupID = "general"
			}
			if msg.Timestamp == 0 {
				msg.Timestamp = time.Now().UnixMilli()
			}
			_ = groups.Put([]byte(msg.GroupID), []byte("1"))
			if msg.Type == "group_announce" || msg.Type == "typing" {
				continue
			}
			if msg.MessageID != "" && idxBucket.Get([]byte(msg.MessageID)) != nil {
				continue // already saved
			}
			bucket, err := tx.CreateBucketIfNotExists([]byte(msg.GroupID))
			if err != nil {
				return err
			}
			key := MakeTimestampKey(msg.Timestamp, msg.MessageID)
			data, err := json.Marshal(msg)
			if err != nil {
				continue
			}
			if err := bucket.Put(key, data); err != nil {
				return err
			}
			if msg.MessageID != "" {
				_ = idxBucket.Put([]byte(msg.MessageID), []byte(msg.GroupID))
			}
		}
		return nil
	})
}

// historySince returns messages in groupID with timestamp > sinceMs using O(log N) cursor seek.
// Returns (messages, hasMore, nextSinceTimestamp).
func historySince(groupID string, sinceMs int64, limit int) ([]ChatMessage, bool, int64) {
	if limit <= 0 {
		limit = 50
	}
	var out []ChatMessage
	var nextSince int64
	hasMore := false

	dbMu.RLock()
	defer dbMu.RUnlock()
	if db == nil {
		return nil, false, 0
	}

	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(groupID))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		seekKey := MakeSeekKey(sinceMs + 1) // strictly newer than sinceMs
		for k, v := c.Seek(seekKey); k != nil; k, v = c.Next() {
			if len(out) >= limit {
				hasMore = true
				break
			}
			var msg ChatMessage
			if err := json.Unmarshal(v, &msg); err == nil {
				out = append(out, msg)
				if msg.Timestamp > nextSince {
					nextSince = msg.Timestamp
				}
			}
		}
		return nil
	})
	return out, hasMore, nextSince
}

// groupHighWatermark returns the highest timestamp observed in each group in O(1) time via cursor.Last().
func groupHighWatermark() map[string]int64 {
	digests := make(map[string]int64)
	dbMu.RLock()
	defer dbMu.RUnlock()
	if db == nil {
		return digests
	}
	db.View(func(tx *bolt.Tx) error {
		groupsB := tx.Bucket([]byte(groupsBucket))
		if groupsB == nil {
			return nil
		}
		return groupsB.ForEach(func(k, v []byte) error {
			groupID := string(k)
			b := tx.Bucket(k)
			if b == nil {
				digests[groupID] = 0
				return nil
			}
			c := b.Cursor()
			lastK, lastV := c.Last()
			if lastK == nil {
				digests[groupID] = 0
				return nil
			}
			if len(lastK) >= 8 {
				digests[groupID] = int64(binary.BigEndian.Uint64(lastK[:8]))
			} else {
				var m ChatMessage
				if err := json.Unmarshal(lastV, &m); err == nil {
					digests[groupID] = m.Timestamp
				}
			}
			return nil
		})
	})
	return digests
}

func knownGroups() []string {
	var groups []string
	dbMu.RLock()
	defer dbMu.RUnlock()
	if db == nil {
		return []string{"general"}
	}
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(groupsBucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			groups = append(groups, string(k))
			return nil
		})
	})
	return groups
}

func groupKnownLocally(groupID string) bool {
	for _, g := range knownGroups() {
		if g == groupID {
			return true
		}
	}
	return false
}

func historyFor(groupID string) []ChatMessage {
	var out []ChatMessage
	dbMu.RLock()
	defer dbMu.RUnlock()
	if db == nil {
		return nil
	}
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(groupID))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var msg ChatMessage
			if err := json.Unmarshal(v, &msg); err == nil {
				out = append(out, msg)
			}
			return nil
		})
	})
	return out
}

type hub struct {
	mu      sync.Mutex
	clients map[*wsConn]bool
}

func newHub() *hub {
	return &hub{clients: make(map[*wsConn]bool)}
}

func (h *hub) add(c *wsConn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = true
}

func (h *hub) remove(c *wsConn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, c)
}

func (h *hub) broadcast(msg ChatMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		if err := c.WriteJSON(msg); err != nil {
			c.Close()
			delete(h.clients, c)
		}
	}
}

type healthStats struct {
	mu           sync.Mutex
	messagesSeen int
	maxHopsSeen  int
	lastActivity time.Time
}

var stats = &healthStats{}

func (s *healthStats) record(hops int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messagesSeen++
	if hops > s.maxHopsSeen {
		s.maxHopsSeen = hops
	}
	s.lastActivity = time.Now()
}

func (s *healthStats) snapshot() (messagesSeen int, maxHops int, lastActivity time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.messagesSeen, s.maxHopsSeen, s.lastActivity
}

func isNetworkAdmin() bool {
	return isAdminLoggingEnabled()
}