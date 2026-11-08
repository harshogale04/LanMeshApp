package core

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestAdminChallengeResponseLifecycle(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "lanmesh-admin-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	secret := "test-admin-secret-12345"
	SetAdminConfig(secret, tempDir, "Admin-HQ")

	if !IsAdminNode() {
		t.Fatal("expected IsAdminNode() to be true")
	}

	mux := http.NewServeMux()
	registerAdminRoutes(mux, nil, func() *bolt.DB { return nil })

	// 1. GET /admin/challenge
	chalReq := httptest.NewRequest("GET", "/admin/challenge", nil)
	chalRec := httptest.NewRecorder()
	mux.ServeHTTP(chalRec, chalReq)

	if chalRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from challenge, got %d: %s", chalRec.Code, chalRec.Body.String())
	}

	var chalBody struct {
		Nonce string `json:"nonce"`
	}
	if err := json.NewDecoder(chalRec.Body).Decode(&chalBody); err != nil {
		t.Fatalf("failed to decode challenge: %v", err)
	}
	if chalBody.Nonce == "" {
		t.Fatal("expected non-empty challenge nonce")
	}

	// 2. Compute HMAC-SHA256(nonce, password)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(chalBody.Nonce))
	hmacHex := hex.EncodeToString(mac.Sum(nil))

	// 3. POST /admin/auth with valid proof
	authPayload, _ := json.Marshal(map[string]string{
		"nonce":    chalBody.Nonce,
		"response": hmacHex,
	})
	authReq := httptest.NewRequest("POST", "/admin/auth", bytes.NewReader(authPayload))
	authReq.Header.Set("Content-Type", "application/json")
	authRec := httptest.NewRecorder()
	mux.ServeHTTP(authRec, authReq)

	if authRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from auth, got %d: %s", authRec.Code, authRec.Body.String())
	}

	cookies := authRec.Result().Cookies()
	var sessionCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == "lanmesh_session" {
			sessionCookie = c
			break
		}
	}
	if sessionCookie == nil || sessionCookie.Value == "" {
		t.Fatal("expected lanmesh_session cookie to be set")
	}
	if !sessionCookie.HttpOnly {
		t.Fatal("expected session cookie to be HttpOnly")
	}

	// 4. Verify Single-Use Nonce: replaying the same nonce must fail!
	replayReq := httptest.NewRequest("POST", "/admin/auth", bytes.NewReader(authPayload))
	replayReq.Header.Set("Content-Type", "application/json")
	replayRec := httptest.NewRecorder()
	mux.ServeHTTP(replayRec, replayReq)

	if replayRec.Code == http.StatusOK {
		t.Fatal("replayed nonce was accepted; expected failure due to single-use requirement")
	}

	// 5. Test Authenticated access to /admin/export.json
	exportReq := httptest.NewRequest("GET", "/admin/export.json", nil)
	exportReq.AddCookie(sessionCookie)
	exportRec := httptest.NewRecorder()
	mux.ServeHTTP(exportRec, exportReq)

	if exportRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from export with session, got %d", exportRec.Code)
	}

	// 6. Test Unauthenticated access to /admin/export.json
	unauthExport := httptest.NewRequest("GET", "/admin/export.json", nil)
	unauthRec := httptest.NewRecorder()
	mux.ServeHTTP(unauthRec, unauthExport)

	if unauthRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized without session, got %d", unauthRec.Code)
	}

	// 7. Test Logout
	logoutReq := httptest.NewRequest("POST", "/admin/logout", nil)
	logoutReq.AddCookie(sessionCookie)
	logoutReq.Header.Set("Accept", "application/json")
	logoutRec := httptest.NewRecorder()
	mux.ServeHTTP(logoutRec, logoutReq)

	if logoutRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from logout, got %d", logoutRec.Code)
	}

	// 8. Verify session is revoked after logout
	postLogoutReq := httptest.NewRequest("GET", "/admin/export.json", nil)
	postLogoutReq.AddCookie(sessionCookie)
	postLogoutRec := httptest.NewRecorder()
	mux.ServeHTTP(postLogoutRec, postLogoutReq)

	if postLogoutRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized after logout, got %d", postLogoutRec.Code)
	}
}

func TestAdminBackfillFromDB(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "lanmesh-backfill-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "lanmesh.db")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Seed messages directly into BoltDB
	err = db.Update(func(tx *bolt.Tx) error {
		gb, _ := tx.CreateBucketIfNotExists([]byte(groupsBucket))
		gb.Put([]byte("general"), []byte("1"))
		gb.Put([]byte("ops"), []byte("1"))

		bGen, _ := tx.CreateBucketIfNotExists([]byte("general"))
		m1 := ChatMessage{
			GroupID:   "general",
			MessageID: "msg-111",
			Payload:   "First backfill message",
			Timestamp: 1000,
			Hops:      1,
			HopTrail:  []string{"peerA"},
		}
		d1, _ := json.Marshal(m1)
		bGen.Put(compositeKey(m1.Timestamp, m1.MessageID), d1)

		bOps, _ := tx.CreateBucketIfNotExists([]byte("ops"))
		m2 := ChatMessage{
			GroupID:   "ops",
			MessageID: "msg-222",
			Payload:   "Second backfill message",
			Timestamp: 2000,
			Hops:      2,
			HopTrail:  []string{"peerA", "peerB"},
		}
		d2, _ := json.Marshal(m2)
		bOps.Put(compositeKey(m2.Timestamp, m2.MessageID), d2)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	logPath := filepath.Join(tempDir, "lanmesh-admin-log.jsonl")
	store, err := newAdminStore(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	if len(store.snapshot()) != 0 {
		t.Fatalf("expected empty store before backfill, got %d", len(store.snapshot()))
	}

	// Run backfill
	if err := store.backfillFromDB(db); err != nil {
		t.Fatalf("backfillFromDB failed: %v", err)
	}

	snap := store.snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 messages after backfill, got %d", len(snap))
	}
	if snap[0].MessageID != "msg-111" || snap[1].MessageID != "msg-222" {
		t.Fatalf("unexpected order or IDs: %+v", snap)
	}

	// Duplicate run must not re-record existing records
	if err := store.backfillFromDB(db); err != nil {
		t.Fatal(err)
	}
	if len(store.snapshot()) != 2 {
		t.Fatalf("expected still 2 messages after second backfill, got %d", len(store.snapshot()))
	}
}

func TestHopTrailTracking(t *testing.T) {
	msg := ChatMessage{
		Type:      "text",
		Payload:   "test hop route",
		Sender:    "12D3KooWOrigin",
		GroupID:   "general",
		MessageID: "1728512340000-a1b2c3d4",
		Hops:      0,
		Timestamp: 1728512340000,
		HopTrail:  []string{"12D3KooWOrigin"},
	}

	// Emulate intermediate relays
	relays := []string{"12D3KooWRelay1", "12D3KooWRelay2"}
	for _, relayID := range relays {
		msg.Hops++
		trail := make([]string, len(msg.HopTrail), len(msg.HopTrail)+1)
		copy(trail, msg.HopTrail)
		trail = append(trail, relayID)
		msg.HopTrail = trail
	}

	if msg.Hops != 2 {
		t.Fatalf("expected 2 hops, got %d", msg.Hops)
	}
	if len(msg.HopTrail) != 3 {
		t.Fatalf("expected 3 trail entries (origin + 2 relays), got %d: %v", len(msg.HopTrail), msg.HopTrail)
	}
	if msg.HopTrail[0] != "12D3KooWOrigin" || msg.HopTrail[1] != "12D3KooWRelay1" || msg.HopTrail[2] != "12D3KooWRelay2" {
		t.Fatalf("unexpected trail order: %v", msg.HopTrail)
	}
}
