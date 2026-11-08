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
	Avatar    string   `json:"avatar,omitempty"`
	FileName  string   `json:"fileName,omitempty"`
	FileSize  int64    `json:"fileSize,omitempty"`
	ReplyTo   string   `json:"replyTo,omitempty"`
	HopTrail  []string `json:"hopTrail,omitempty"`
	To        string   `json:"to,omitempty"`
}

const maxHops = 6
const protocolID = "/p2p-chat/1.0.0"

// callProtocolID carries live call signaling only (1:1 and group room calls).
// It deliberately bypasses markSeen/saveMessage/relayToPeers: signaling
// is ephemeral and point-to-point between directly connected peers,
// not persisted chat history and never multi-hop relayed.
const callProtocolID = "/lanmesh-call/1.0.0"

// relaySem is a bounded semaphore (capacity 32) limiting simultaneous
// outbound forwarding streams across the mesh, protecting CPU and battery
// during message bursts.
var relaySem = make(chan struct{}, 32)

func isCallSignal(t string) bool {
	switch t {
	case "call-offer", "call-answer", "call-ice", "call-end",
		"room-call-start", "room-call-join", "room-call-leave", "room-call-presence", "room-call-end",
		"room-offer", "room-answer", "room-ice",
		"peer-identity": // Ephemeral display-name exchange
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

func relayToPeers(ctx context.Context, host libp2phost.Host, exclude peer.ID, msg ChatMessage) {
	if msg.Hops >= maxHops {
		return
	}
	msgCopy := msg
	msgCopy.Hops++

	// Append current node to the hop trail (copy so the original is untouched,
	// and avoid duplicating our own ID if we are already the last hop)
	myID := host.ID().String()
	trail := make([]string, len(msg.HopTrail), len(msg.HopTrail)+1)
	copy(trail, msg.HopTrail)
	if len(trail) == 0 || trail[len(trail)-1] != myID {
		trail = append(trail, myID)
	}
	msgCopy.HopTrail = trail

	data, err := json.Marshal(msgCopy)
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

func fnv1a64(s string) uint64 {
	var h uint64 = 0xcbf29ce484222325
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 0x100000001b3
	}
	return h
}

// compositeKey returns a 16-byte index key:
// [8 Bytes: UnixMs Timestamp (BigEndian)] [8 Bytes: FNV-1a Hash of Message ID]
func compositeKey(ts int64, msgID string) []byte {
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	}
	k := make([]byte, 16)
	binary.BigEndian.PutUint64(k[0:8], uint64(ts))
	binary.BigEndian.PutUint64(k[8:16], fnv1a64(msgID))
	return k
}

// seekKeyFor returns a 16-byte cursor seek key positioned at the first entry
// with timestamp >= ts (the hash half is zero, so it sorts before any real key).
func seekKeyFor(ts int64) []byte {
	if ts < 0 {
		ts = 0
	}
	k := make([]byte, 16)
	binary.BigEndian.PutUint64(k[0:8], uint64(ts))
	return k
}

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
	if msg.Timestamp <= 0 {
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
		if msg.Type == "group_announce" || msg.Type == "typing" || msg.Type == "ping" {
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

		key := compositeKey(msg.Timestamp, msg.MessageID)
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
			if msg.Timestamp <= 0 {
				msg.Timestamp = time.Now().UnixMilli()
			}
			_ = groups.Put([]byte(msg.GroupID), []byte("1"))
			if msg.Type == "group_announce" || msg.Type == "typing" || msg.Type == "ping" {
				continue
			}
			if msg.MessageID != "" && idxBucket.Get([]byte(msg.MessageID)) != nil {
				continue // already saved
			}
			bucket, err := tx.CreateBucketIfNotExists([]byte(msg.GroupID))
			if err != nil {
				return err
			}
			key := compositeKey(msg.Timestamp, msg.MessageID)
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
		seekKey := seekKeyFor(sinceMs + 1) // strictly newer than sinceMs
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
	if len(groups) == 0 {
		return []string{"general"}
	}
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

func historyForSeek(groupID string, since int64, limit int) []ChatMessage {
	if limit <= 0 {
		limit = 50
	}
	var out []ChatMessage
	dbMu.RLock()
	defer dbMu.RUnlock()
	if db == nil {
		return out
	}
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(groupID))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		var k, v []byte
		if since > 0 {
			k, v = c.Seek(seekKeyFor(since + 1))
		} else {
			k, v = c.First()
		}
		for ; k != nil && len(out) < limit; k, v = c.Next() {
			var msg ChatMessage
			if err := json.Unmarshal(v, &msg); err == nil {
				out = append(out, msg)
			}
		}
		return nil
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
	return IsAdminNode()
}