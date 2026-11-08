package core

import (
	"bufio"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

type adminStore struct {
	mu       sync.Mutex
	file     *os.File
	messages []ChatMessage
	seenIDs  map[string]bool
}

func newAdminStore(path string) (*adminStore, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	store := &adminStore{
		file:    f,
		seenIDs: make(map[string]bool),
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		var m ChatMessage
		if err := json.Unmarshal(scanner.Bytes(), &m); err == nil {
			store.messages = append(store.messages, m)
			if m.MessageID != "" {
				store.seenIDs[m.MessageID] = true
			}
		}
	}
	return store, nil
}

func (a *adminStore) close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.file != nil {
		err := a.file.Close()
		a.file = nil
		return err
	}
	return nil
}

// isEphemeralType reports message types that must not enter the forensic audit log.
func isEphemeralType(t string) bool {
	return t == "typing" || t == "ping" || t == "whoami" || t == "sync_status"
}

func (a *adminStore) record(msg ChatMessage) {
	if a == nil || msg.MessageID == "" {
		return
	}
	// Filter ephemeral pings and typing indicators from forensic audit log
	if isEphemeralType(msg.Type) {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.seenIDs[msg.MessageID] {
		return
	}
	a.seenIDs[msg.MessageID] = true
	a.messages = append(a.messages, msg)
	if a.file != nil {
		if data, err := json.Marshal(msg); err == nil {
			a.file.Write(data)
			a.file.Write([]byte("\n"))
		}
	}
}

// backfillFromHistory scans all known local groups (via knownGroups/historyFor)
// and ingests any unlogged messages. Returns the number of messages added.
func (a *adminStore) backfillFromHistory() int {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	backfilled := 0
	for _, g := range knownGroups() {
		for _, m := range historyFor(g) {
			if m.MessageID == "" || a.seenIDs[m.MessageID] {
				continue
			}
			if isEphemeralType(m.Type) {
				continue
			}
			a.seenIDs[m.MessageID] = true
			a.messages = append(a.messages, m)
			if a.file != nil {
				if data, err := json.Marshal(m); err == nil {
					a.file.Write(data)
					a.file.Write([]byte("\n"))
				}
			}
			backfilled++
		}
	}
	return backfilled
}

func (a *adminStore) snapshot() []ChatMessage {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]ChatMessage, len(a.messages))
	copy(out, a.messages)
	return out
}

// backfillFromDB scans all local group buckets in BoltDB and records any
// messages missing from the forensic audit log into lanmesh-admin-log.jsonl.
func (a *adminStore) backfillFromDB(database *bolt.DB) error {
	if a == nil || database == nil {
		return nil
	}
	var newMsgs []ChatMessage
	err := database.View(func(tx *bolt.Tx) error {
		groupsB := tx.Bucket([]byte(groupsBucket))
		if groupsB == nil {
			return nil
		}
		return groupsB.ForEach(func(k, v []byte) error {
			groupName := string(k)
			b := tx.Bucket([]byte(groupName))
			if b == nil {
				return nil
			}
			return b.ForEach(func(msgKey, msgVal []byte) error {
				var m ChatMessage
				if err := json.Unmarshal(msgVal, &m); err == nil && m.MessageID != "" {
					newMsgs = append(newMsgs, m)
				}
				return nil
			})
		})
	})
	if err != nil {
		return err
	}

	sort.Slice(newMsgs, func(i, j int) bool {
		return newMsgs[i].Timestamp < newMsgs[j].Timestamp
	})

	for _, m := range newMsgs {
		a.record(m)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

var (
	adminMu         sync.RWMutex
	adminStor       *adminStore
	adminDir        string
	adminSecret     string
	isAdminNodeFlag bool
	defaultNickname string

	// Nonce and session storage
	nonceMu  sync.Mutex
	nonces   = make(map[string]time.Time) // nonce -> creation time
	sessMu   sync.Mutex
	sessions = make(map[string]time.Time) // session token -> expiry
)

// SetAdminConfig configures administrative role, secret, and working paths.
func SetAdminConfig(secret, dir, nickname string) {
	adminMu.Lock()
	defer adminMu.Unlock()
	adminSecret = strings.TrimSpace(secret)
	isAdminNodeFlag = adminSecret != ""
	if dir != "" {
		adminDir = dir
	}
	if nickname != "" {
		defaultNickname = nickname
	} else if isAdminNodeFlag {
		defaultNickname = "Admin-HQ"
	}
}

// setAdminDataDir sets the directory used for admin logs and the secret file.
func setAdminDataDir(dir string) {
	adminMu.Lock()
	adminDir = dir
	adminMu.Unlock()
}

// setAdminSecret sets the admin secret and marks this node as an admin station.
func setAdminSecret(secret string) {
	adminMu.Lock()
	adminSecret = strings.TrimSpace(secret)
	isAdminNodeFlag = adminSecret != ""
	adminMu.Unlock()
}

func getAdminSecret() string {
	adminMu.RLock()
	defer adminMu.RUnlock()
	return adminSecret
}

func IsAdminNode() bool {
	adminMu.RLock()
	defer adminMu.RUnlock()
	return isAdminNodeFlag
}

func GetDefaultNickname() string {
	adminMu.RLock()
	defer adminMu.RUnlock()
	if defaultNickname != "" {
		return defaultNickname
	}
	if isAdminNodeFlag {
		return "Admin-HQ"
	}
	return "Peer"
}

func currentAdminStore() *adminStore {
	adminMu.RLock()
	defer adminMu.RUnlock()
	return adminStor
}

func enableAdminLogging() (*adminStore, error) {
	adminMu.Lock()
	defer adminMu.Unlock()
	if adminStor != nil {
		// Run backfill
		adminStor.backfillFromHistory()
		return adminStor, nil
	}
	if adminDir == "" {
		adminDir = "."
	}
	logPath := filepath.Join(adminDir, "lanmesh-admin-log.jsonl")
	s, err := newAdminStore(logPath)
	if err != nil {
		return nil, err
	}
	adminStor = s
	s.backfillFromHistory()
	fmt.Println("[LANMESH] Admin audit logging active — writing forensic audit trail to", logPath)
	return adminStor, nil
}

func isAdminLoggingEnabled() bool {
	return currentAdminStore() != nil
}

func generateRandomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func createChallengeNonce() string {
	nonceMu.Lock()
	defer nonceMu.Unlock()

	// Clean up nonces older than 2 minutes
	now := time.Now()
	for k, exp := range nonces {
		if now.Sub(exp) > 2*time.Minute {
			delete(nonces, k)
		}
	}

	nonce := generateRandomHex(32)
	nonces[nonce] = now
	return nonce
}

func verifyAndConsumeNonce(nonce string) bool {
	if nonce == "" {
		return false
	}
	nonceMu.Lock()
	defer nonceMu.Unlock()

	created, ok := nonces[nonce]
	if !ok {
		return false
	}
	delete(nonces, nonce) // Single-use!
	return time.Since(created) <= 2*time.Minute
}

func createSession() string {
	sessMu.Lock()
	defer sessMu.Unlock()

	token := generateRandomHex(32)
	sessions[token] = time.Now().Add(8 * time.Hour)
	return token
}

func isAdminAuthenticated(r *http.Request) bool {
	cookie, err := r.Cookie("lanmesh_session")
	if err != nil || cookie.Value == "" {
		return false
	}

	sessMu.Lock()
	defer sessMu.Unlock()

	exp, ok := sessions[cookie.Value]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(sessions, cookie.Value)
		return false
	}
	return true
}

func revokeSession(r *http.Request) {
	if cookie, err := r.Cookie("lanmesh_session"); err == nil && cookie.Value != "" {
		sessMu.Lock()
		delete(sessions, cookie.Value)
		sessMu.Unlock()
	}
}

func registerAdminRoutes(mux *http.ServeMux, broadcastFn func(ChatMessage), getDB func() *bolt.DB) {
	// POST /admin/configure - Configures admin secret dynamically
	mux.HandleFunc("/admin/configure", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Secret string `json:"secret"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		req.Secret = strings.TrimSpace(req.Secret)
		if req.Secret == "" {
			http.Error(w, "secret cannot be empty", http.StatusBadRequest)
			return
		}

		adminMu.Lock()
		adminSecret = req.Secret
		isAdminNodeFlag = true
		if defaultNickname == "" || defaultNickname == "Peer" {
			defaultNickname = "Admin-HQ"
		}
		secretPath := filepath.Join(adminDir, "admin.secret")
		_ = os.WriteFile(secretPath, []byte(req.Secret), 0600)
		adminMu.Unlock()

		_, _ = enableAdminLogging()

		dbMu.RLock()
		currentDB := db
		dbMu.RUnlock()
		if currentDB != nil {
			if store := currentAdminStore(); store != nil {
				_ = store.backfillFromDB(currentDB)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "configured",
			"isAdmin": true,
		})
	})

	// GET /admin/challenge - Issues single-use cryptographic nonce with 2-minute TTL
	mux.HandleFunc("/admin/challenge", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !IsAdminNode() {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]string{"error": "node not configured as admin station"})
			return
		}
		nonce := createChallengeNonce()
		json.NewEncoder(w).Encode(map[string]string{"nonce": nonce})
	})

	// POST /admin/auth - Validates HMAC response and issues 8-hour HttpOnly session cookie
	mux.HandleFunc("/admin/auth", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")

		if !IsAdminNode() {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]string{"error": "node not configured as admin station"})
			return
		}

		var req struct {
			Nonce    string `json:"nonce"`
			Response string `json:"response"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "malformed JSON request"})
			return
		}

		if !verifyAndConsumeNonce(req.Nonce) {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "challenge nonce invalid or expired (2m TTL)"})
			return
		}

		secret := getAdminSecret()

		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(req.Nonce))
		expectedHex := hex.EncodeToString(mac.Sum(nil))

		if subtle.ConstantTimeCompare([]byte(strings.ToLower(req.Response)), []byte(strings.ToLower(expectedHex))) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid admin secret credentials"})
			return
		}

		sessionToken := createSession()
		http.SetCookie(w, &http.Cookie{
			Name:     "lanmesh_session",
			Value:    sessionToken,
			Path:     "/",
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
			MaxAge:   8 * 3600,
			Expires:  time.Now().Add(8 * time.Hour),
		})

		// Start forensic logging (and backfill) once an admin has authenticated
		_, _ = enableAdminLogging()

		json.NewEncoder(w).Encode(map[string]string{"status": "authenticated"})
	})

	// POST /admin/logout - Revokes current session cookie
	mux.HandleFunc("/admin/logout", func(w http.ResponseWriter, r *http.Request) {
		revokeSession(r)
		http.SetCookie(w, &http.Cookie{
			Name:     "lanmesh_session",
			Value:    "",
			Path:     "/",
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
			MaxAge:   -1,
			Expires:  time.Unix(0, 0),
		})
		if r.Header.Get("Accept") == "application/json" || strings.Contains(r.Header.Get("Content-Type"), "json") {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "logged_out"})
			return
		}
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
	})

	// GET /admin/status - Machine-readable status ({enabled, isAdminNode, sessionActive})
	mux.HandleFunc("/admin/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"enabled":       isAdminLoggingEnabled(),
			"isAdminNode":   IsAdminNode(),
			"sessionActive": isAdminAuthenticated(r),
		})
	})

	// GET /is-admin - Capability and session status check
	mux.HandleFunc("/is-admin", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"isAdmin":         IsAdminNode(),
			"sessionActive":   isAdminAuthenticated(r),
			"defaultNickname": GetDefaultNickname(),
		})
	})

	// GET /admin/export.json - Downloads forensic audit trail as JSON array (Session cookie required)
	mux.HandleFunc("/admin/export.json", func(w http.ResponseWriter, r *http.Request) {
		if !isAdminAuthenticated(r) {
			http.Error(w, "unauthorized: valid admin session required", http.StatusUnauthorized)
			return
		}
		store := currentAdminStore()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", "attachment; filename=\"lanmesh-admin-export.json\"")
		if store == nil {
			w.Write([]byte("[]"))
			return
		}
		json.NewEncoder(w).Encode(store.snapshot())
	})

	// POST /admin/groups - Creates and broadcasts a new group channel across the mesh (Session cookie required)
	mux.HandleFunc("/admin/groups", func(w http.ResponseWriter, r *http.Request) {
		if !isAdminAuthenticated(r) {
			http.Error(w, "unauthorized: valid admin session required", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}

		var groupName string
		if strings.Contains(r.Header.Get("Content-Type"), "application/json") {
			var body struct {
				Name string `json:"name"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			groupName = body.Name
		} else {
			groupName = r.FormValue("name")
		}

		groupName = strings.TrimSpace(strings.ToLower(groupName))
		if groupName == "" {
			http.Error(w, "group name required", http.StatusBadRequest)
			return
		}

		// Persist group channel locally
		dbMu.RLock()
		database := db
		dbMu.RUnlock()
		if database != nil {
			_ = database.Update(func(tx *bolt.Tx) error {
				groups, err := tx.CreateBucketIfNotExists([]byte(groupsBucket))
				if err != nil {
					return err
				}
				return groups.Put([]byte(groupName), []byte("1"))
			})
		}

		// Broadcast group announcement frame across the mesh
		if broadcastFn != nil {
			broadcastFn(ChatMessage{
				Type:      "group_announce",
				Payload:   groupName,
				GroupID:   groupName,
				MessageID: newMessageID(),
				Timestamp: time.Now().UnixMilli(),
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "created",
			"group":  groupName,
		})
	})

	// GET /admin - Admin dashboard (displays zero-knowledge login if unauthenticated)
	mux.HandleFunc("/admin", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")

		// 1. If this device is NOT an admin station:
		if !IsAdminNode() {
			fmt.Fprint(w, `<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>LANMESH — Admin Station</title>
<style>
body{background:#0b0b0c;color:#eee;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;padding:32px;display:flex;align-items:center;justify-content:center;min-height:80vh;}
.box{background:#161618;border:1px solid #2a2a2e;border-radius:18px;max-width:440px;width:100%;padding:28px;text-align:center;box-shadow:0 20px 40px rgba(0,0,0,0.5);}
h1{color:#f2c230;font-size:1.3rem;margin:0 0 12px;}
p{color:#8e8e93;font-size:0.9rem;line-height:1.5;margin:0 0 20px;}
code{background:#262629;color:#30d158;padding:3px 6px;border-radius:6px;font-size:0.85rem;}
a{color:#0a84ff;text-decoration:none;}
</style></head><body>
<div class="box">
<h1>Standard Peer Node</h1>
<p>This LANMesh instance is running as a standard decentralized peer node. Admin audit capabilities are disabled.</p>
<p>To enable the authoritative admin station, run the daemon with <code>-admin-secret &lt;secret&gt;</code> or set the <code>LANMESH_ADMIN_SECRET</code> environment variable.</p>
<p><a href="/">← Return to Mesh Chat</a></p>
</div></body></html>`)
			return
		}

		// 2. If admin station is configured but user is NOT authenticated:
		if !isAdminAuthenticated(r) {
			fmt.Fprint(w, `<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>LANMESH — Admin Authentication</title>
<style>
body{background:#0b0b0c;color:#eee;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;padding:24px;display:flex;align-items:center;justify-content:center;min-height:80vh;}
.card{background:#161618;border:1px solid #2c2c2e;border-radius:20px;max-width:420px;width:100%;padding:32px;box-shadow:0 24px 50px rgba(0,0,0,0.6);}
.brand{color:#f2c230;font-size:1.4rem;font-weight:700;margin:0 0 6px;}
.subtitle{color:#8e8e93;font-size:0.85rem;margin:0 0 24px;}
.badge{display:inline-block;background:#1e1e24;color:#f2c230;border:1px solid #3a3a3c;font-size:0.75rem;padding:3px 10px;border-radius:999px;margin-bottom:18px;}
label{display:block;font-size:0.8rem;text-transform:uppercase;color:#8e8e93;margin-bottom:8px;font-weight:600;}
input[type="password"]{width:100%;padding:12px 14px;background:#262629;border:1px solid #3a3a3c;border-radius:10px;color:#fff;font-size:1rem;margin-bottom:18px;box-sizing:border-box;}
input[type="password"]:focus{outline:none;border-color:#0a84ff;}
button{width:100%;padding:12px;background:#0a84ff;color:#fff;border:none;border-radius:10px;font-size:0.95rem;font-weight:600;cursor:pointer;transition:opacity .15s;}
button:hover{opacity:0.9;}
button:disabled{opacity:0.5;cursor:not-allowed;}
.err{color:#ff453a;font-size:0.85rem;margin-top:14px;display:none;background:rgba(255,69,58,0.1);padding:10px;border-radius:8px;}
.note{color:#636366;font-size:0.75rem;margin-top:20px;text-align:center;line-height:1.4;}
a{color:#0a84ff;text-decoration:none;font-size:0.85rem;}
</style></head><body>
<div class="card">
<div class="brand">LANMESH HQ</div>
<div class="subtitle">Authoritative Admin & Forensic Audit Station</div>
<span class="badge">🔒 Zero-Knowledge Authentication</span>

<form id="authForm" onsubmit="handleLogin(event)">
<label for="pwd">Admin Secret</label>
<input type="password" id="pwd" placeholder="Enter admin password" required autocomplete="current-password" autofocus>
<button type="submit" id="btn">Verify & Authenticate</button>
<div id="errMsg" class="err"></div>
</form>

<div class="note">Password never travels over the wire. Authenticates via local WebCrypto HMAC-SHA256 challenge-response.</div>
<p style="text-align:center;margin-top:16px;"><a href="/">← Return to Mesh Chat</a></p>
</div>

<script>
async function handleLogin(e) {
  e.preventDefault();
  const pwd = document.getElementById('pwd').value;
  const btn = document.getElementById('btn');
  const errDiv = document.getElementById('errMsg');
  errDiv.style.display = 'none';
  btn.disabled = true;
  btn.textContent = 'Generating proof...';

  try {
    // 1. Fetch single-use nonce (2m TTL)
    const cRes = await fetch('/admin/challenge');
    if (!cRes.ok) throw new Error('Failed to acquire challenge nonce from server');
    const { nonce } = await cRes.json();

    // 2. Compute HMAC-SHA256(nonce, password) locally using WebCrypto API
    const enc = new TextEncoder();
    const keyData = enc.encode(pwd);
    const cryptoKey = await window.crypto.subtle.importKey(
      'raw',
      keyData,
      { name: 'HMAC', hash: { name: 'SHA-256' } },
      false,
      ['sign']
    );
    const nonceData = enc.encode(nonce);
    const signature = await window.crypto.subtle.sign('HMAC', cryptoKey, nonceData);
    const hashArr = Array.from(new Uint8Array(signature));
    const hmacHex = hashArr.map(b => b.toString(16).padStart(2, '0')).join('');

    // 3. Post proof
    btn.textContent = 'Authenticating...';
    const aRes = await fetch('/admin/auth', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ nonce, response: hmacHex })
    });

    if (!aRes.ok) {
      const respData = await aRes.json().catch(() => ({}));
      throw new Error(respData.error || 'Authentication rejected: invalid admin credentials');
    }

    // 4. Reload to render authenticated dashboard
    window.location.reload();
  } catch (err) {
    errDiv.textContent = err.message;
    errDiv.style.display = 'block';
    btn.disabled = false;
    btn.textContent = 'Verify & Authenticate';
  }
}
</script>
</body></html>`)
			return
		}

		// 3. Authenticated Admin Dashboard:
		store := currentAdminStore()
		var msgs []ChatMessage
		if store != nil {
			msgs = store.snapshot()
			sort.Slice(msgs, func(i, j int) bool { return msgs[i].Timestamp > msgs[j].Timestamp })
		}

		fmt.Fprint(w, `<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>LANMESH — Authoritative Admin Dashboard</title>
<style>
body{background:#0b0b0c;color:#eee;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;padding:24px;margin:0;}
header{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:24px;border-bottom:1px solid #222;padding-bottom:16px;}
.title-group h1{color:#f2c230;font-size:1.3rem;margin:0 0 4px;}
.title-group p{color:#8e8e93;font-size:0.85rem;margin:0;}
.badge{background:#1b1b1c;border:1px solid #2a2a2a;color:#f2c230;padding:3px 10px;border-radius:999px;font-size:0.75rem;font-weight:600;}
.actions{display:flex;align-items:center;gap:10px;}
.btn{background:#0a84ff;color:#fff;border:none;padding:8px 14px;border-radius:8px;font-size:0.85rem;font-weight:600;cursor:pointer;text-decoration:none;display:inline-flex;align-items:center;gap:6px;}
.btn:hover{opacity:0.9;}
.btn-danger{background:#ff453a;}
.btn-secondary{background:#2c2c2e;color:#ddd;}
.stats-bar{display:flex;gap:16px;margin-bottom:20px;flex-wrap:wrap;}
.stat-card{background:#161618;border:1px solid #262629;border-radius:12px;padding:12px 18px;min-width:140px;}
.stat-num{font-size:1.4rem;font-weight:700;color:#f2c230;}
.stat-label{font-size:0.75rem;color:#8e8e93;text-transform:uppercase;margin-top:2px;}
.create-group{background:#161618;border:1px solid #262629;border-radius:12px;padding:14px 18px;margin-bottom:24px;display:flex;align-items:center;gap:12px;flex-wrap:wrap;}
.create-group input{background:#262629;border:1px solid #3a3a3c;color:#fff;padding:8px 12px;border-radius:8px;font-size:0.85rem;width:200px;}
table{width:100%;border-collapse:collapse;font-size:0.85rem;background:#161618;border-radius:12px;overflow:hidden;box-shadow:0 4px 12px rgba(0,0,0,0.3);}
th,td{border-bottom:1px solid #222;padding:10px 14px;text-align:left;vertical-align:middle;}
th{background:#1c1c1e;color:#8e8e93;text-transform:uppercase;font-size:0.7rem;letter-spacing:0.04em;}
tr:hover td{background:#1a1a1d;}
.hop-badge{display:inline-block;padding:2px 8px;border-radius:999px;font-size:0.75rem;background:#262629;color:#30d158;font-weight:600;}
.hop-badge.relayed{color:#0a84ff;background:#1b2430;}
.trail{color:#8e8e93;font-family:monospace;font-size:0.75rem;max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.content-cell{max-width:320px;word-break:break-word;}
</style></head><body>

<header>
<div class="title-group">
<h1>LANMESH Forensic Audit Reference</h1>
<p>Authoritative Station &middot; <span class="badge">Session Active (8h)</span> &middot; <a href="/" style="color:#0a84ff;text-decoration:none;">Open Mesh Chat &rarr;</a></p>
</div>
<div class="actions">
<a href="/admin/export.json" class="btn btn-secondary">Export JSON</a>
<form method="POST" action="/admin/logout" style="margin:0;">
<button type="submit" class="btn btn-danger">Logout</button>
</form>
</div>
</header>

<div class="stats-bar">
<div class="stat-card">
<div class="stat-num">` + fmt.Sprint(len(msgs)) + `</div>
<div class="stat-label">Audit Records</div>
</div>
<div class="stat-card">
<div class="stat-num">` + html.EscapeString(GetDefaultNickname()) + `</div>
<div class="stat-label">Roster Identity</div>
</div>
<div class="stat-card">
<div class="stat-num">6 Max</div>
<div class="stat-label">Hopping TTL Boundary</div>
</div>
</div>

<form class="create-group" onsubmit="handleCreateGroup(event)">
<span style="font-size:0.85rem;font-weight:600;color:#ddd;">Create Mesh Channel:</span>
<input type="text" id="groupInput" placeholder="channel-name (e.g. ops)" required>
<button type="submit" class="btn">Broadcast Channel</button>
<span id="groupStatus" style="font-size:0.8rem;color:#30d158;"></span>
</form>

<table>
<tr>
<th>Timestamp</th>
<th>Group</th>
<th>Sender</th>
<th>Nickname</th>
<th>Type</th>
<th>Hops</th>
<th>Hop Trail (Forensic Route)</th>
<th>Content Payload</th>
</tr>`)

		for _, m := range msgs {
			ts := ""
			if m.Timestamp > 0 {
				ts = time.UnixMilli(m.Timestamp).Format("15:04:05.000")
			}
			preview := m.Payload
			if m.Type != "text" {
				preview = "[" + m.Type + "]"
			} else {
				preview = truncate(preview, 200)
			}

			hopClass := "hop-badge"
			hopText := fmt.Sprintf("⚡ Direct (%d)", m.Hops)
			if m.Hops > 1 {
				hopClass += " relayed"
				hopText = fmt.Sprintf("🔄 Relay (%d)", m.Hops)
			}

			trailStr := "Direct"
			if len(m.HopTrail) > 0 {
				shortTrail := make([]string, len(m.HopTrail))
				for idx, p := range m.HopTrail {
					if len(p) > 8 {
						p = p[:8] + "..."
					}
					shortTrail[idx] = html.EscapeString(p)
				}
				trailStr = strings.Join(shortTrail, " &rarr; ")
			}

			fmt.Fprintf(w, "<tr><td>%s</td><td><span class=\"badge\">#%s</span></td><td><code>%s</code></td><td>%s</td><td>%s</td><td><span class=\"%s\">%s</span></td><td class=\"trail\" title=\"%s\">%s</td><td class=\"content-cell\">%s</td></tr>",
				html.EscapeString(ts),
				html.EscapeString(m.GroupID),
				html.EscapeString(truncate(m.Sender, 12)),
				html.EscapeString(m.Nickname),
				html.EscapeString(m.Type),
				hopClass,
				hopText,
				html.EscapeString(strings.Join(m.HopTrail, " -> ")),
				trailStr,
				html.EscapeString(preview),
			)
		}

		fmt.Fprint(w, `</table>

<script>
async function handleCreateGroup(e) {
  e.preventDefault();
  const input = document.getElementById('groupInput');
  const status = document.getElementById('groupStatus');
  const name = input.value.trim();
  if (!name) return;
  status.textContent = 'Announcing...';
  try {
    const res = await fetch('/admin/groups', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name })
    });
    if (!res.ok) throw new Error('Failed to create group');
    status.textContent = 'Channel #' + name + ' broadcast!';
    input.value = '';
    setTimeout(() => { status.textContent = ''; }, 3000);
  } catch (err) {
    status.style.color = '#ff453a';
    status.textContent = err.message;
  }
}
</script>
</body></html>`)
	})
}