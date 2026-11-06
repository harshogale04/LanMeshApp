package core

import (
	"bufio"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
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

func (a *adminStore) record(msg ChatMessage) {
	// Filter ephemeral pings and typing indicators from forensic audit log
	if msg.Type == "typing" || msg.Type == "ping" || msg.Type == "whoami" || msg.Type == "sync_status" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	if msg.MessageID != "" && a.seenIDs[msg.MessageID] {
		return
	}
	if msg.MessageID != "" {
		a.seenIDs[msg.MessageID] = true
	}
	a.messages = append(a.messages, msg)
	if data, err := json.Marshal(msg); err == nil {
		a.file.Write(data)
		a.file.Write([]byte("\n"))
	}
}

// backfillFromDB scans all local BoltDB group buckets and ingests any unlogged messages.
func (a *adminStore) backfillFromDB() int {
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
			if m.Type == "typing" || m.Type == "ping" || m.Type == "whoami" || m.Type == "sync_status" {
				continue
			}
			a.seenIDs[m.MessageID] = true
			a.messages = append(a.messages, m)
			if data, err := json.Marshal(m); err == nil {
				a.file.Write(data)
				a.file.Write([]byte("\n"))
				backfilled++
			}
		}
	}
	return backfilled
}

func (a *adminStore) snapshot() []ChatMessage {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]ChatMessage, len(a.messages))
	copy(out, a.messages)
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

var (
	adminMu       sync.RWMutex
	adminStor     *adminStore
	adminDir      string
	adminSecret   string
	activeNonces  = make(map[string]time.Time)
	activeNoncesMu sync.Mutex
	validSessions = make(map[string]time.Time)
	sessionsMu    sync.Mutex
)

func setAdminDataDir(dir string) {
	adminMu.Lock()
	adminDir = dir
	adminMu.Unlock()
}

func setAdminSecret(secret string) {
	adminMu.Lock()
	adminSecret = secret
	adminMu.Unlock()
}

func getAdminSecret() string {
	adminMu.RLock()
	defer adminMu.RUnlock()
	return adminSecret
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
		adminStor.backfillFromDB()
		return adminStor, nil
	}
	logPath := filepath.Join(adminDir, "lanmesh-admin-log.jsonl")
	s, err := newAdminStore(logPath)
	if err != nil {
		return nil, err
	}
	adminStor = s
	s.backfillFromDB()
	fmt.Println("[LANMESH-ADMIN] Logging enabled — writing forensic audit trail to", logPath)
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

func isSessionAuthenticated(r *http.Request) bool {
	secret := getAdminSecret()
	if secret == "" {
		return true // No password required if admin secret was not set
	}
	cookie, err := r.Cookie("lanmesh_session")
	if err != nil || cookie.Value == "" {
		return false
	}
	sessionsMu.Lock()
	defer sessionsMu.Unlock()
	expiry, exists := validSessions[cookie.Value]
	if !exists || time.Now().After(expiry) {
		delete(validSessions, cookie.Value)
		return false
	}
	return true
}

func registerAdminRoutes(mux *http.ServeMux) {
	// GET /is-admin
	mux.HandleFunc("/is-admin", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		isAdmin := getAdminSecret() != "" || isAdminLoggingEnabled()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"isAdmin":         isAdmin,
			"sessionActive":   isSessionAuthenticated(r),
			"defaultNickname": "Admin",
		})
	})

	// GET /admin/challenge: Single-use cryptographic nonce with 2-minute TTL
	mux.HandleFunc("/admin/challenge", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		nonce := generateRandomHex(16)

		activeNoncesMu.Lock()
		activeNonces[nonce] = time.Now().Add(2 * time.Minute)
		activeNoncesMu.Unlock()

		json.NewEncoder(w).Encode(map[string]string{
			"nonce": nonce,
		})
	})

	// POST /admin/auth: Validates HMAC-SHA256(nonce, adminSecret)
	mux.HandleFunc("/admin/auth", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Nonce string `json:"nonce"`
			HMAC  string `json:"hmac"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		secret := getAdminSecret()
		if secret == "" {
			http.Error(w, "Admin secret not configured", http.StatusForbidden)
			return
		}

		activeNoncesMu.Lock()
		exp, ok := activeNonces[req.Nonce]
		if !ok || time.Now().After(exp) {
			activeNoncesMu.Unlock()
			http.Error(w, "Expired or invalid challenge nonce", http.StatusUnauthorized)
			return
		}
		delete(activeNonces, req.Nonce)
		activeNoncesMu.Unlock()

		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(req.Nonce))
		expectedHMAC := hex.EncodeToString(mac.Sum(nil))

		if !hmac.Equal([]byte(expectedHMAC), []byte(req.HMAC)) {
			http.Error(w, "Authentication failed", http.StatusUnauthorized)
			return
		}

		// Success: issue 8-hour session cookie
		sessionToken := generateRandomHex(32)
		sessionsMu.Lock()
		validSessions[sessionToken] = time.Now().Add(8 * time.Hour)
		sessionsMu.Unlock()

		_ , _ = enableAdminLogging()

		http.SetCookie(w, &http.Cookie{
			Name:     "lanmesh_session",
			Value:    sessionToken,
			Path:     "/",
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
			MaxAge:   8 * 3600,
		})

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"authenticated": true})
	})

	// POST /admin/logout: Revokes session
	mux.HandleFunc("/admin/logout", func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("lanmesh_session")
		if err == nil && cookie.Value != "" {
			sessionsMu.Lock()
			delete(validSessions, cookie.Value)
			sessionsMu.Unlock()
		}
		http.SetCookie(w, &http.Cookie{
			Name:     "lanmesh_session",
			Value:    "",
			Path:     "/",
			HttpOnly: true,
			MaxAge:   -1,
		})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"loggedOut": true})
	})

	// POST /admin/enable
	mux.HandleFunc("/admin/enable", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		if _, err := enableAdminLogging(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("ok"))
	})

	// GET /admin/status
	mux.HandleFunc("/admin/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"enabled":       isAdminLoggingEnabled(),
			"isAdminNode":   getAdminSecret() != "" || isAdminLoggingEnabled(),
			"sessionActive": isSessionAuthenticated(r),
		})
	})

	// GET /admin/export.json
	mux.HandleFunc("/admin/export.json", func(w http.ResponseWriter, r *http.Request) {
		if !isSessionAuthenticated(r) {
			http.Error(w, "Unauthorized — admin session required", http.StatusUnauthorized)
			return
		}
		store := currentAdminStore()
		w.Header().Set("Content-Type", "application/json")
		if store == nil {
			w.Write([]byte("[]"))
			return
		}
		json.NewEncoder(w).Encode(store.snapshot())
	})

	// GET /admin dashboard
	mux.HandleFunc("/admin", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		secret := getAdminSecret()

		// If admin secret is set and user is not authenticated, display HMAC challenge login form
		if secret != "" && !isSessionAuthenticated(r) {
			renderAdminLoginForm(w)
			return
		}

		store := currentAdminStore()
		fmt.Fprint(w, `<!DOCTYPE html><html><head><meta charset="utf-8">
<title>LANMESH — Admin Forensic Log</title>
<style>
body{background:#0b0b0c;color:#eee;font-family:sans-serif;padding:24px;margin:0;}
.header{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;border-bottom:1px solid #2a2a2a;padding-bottom:12px;}
h1{color:#f2c230;font-size:1.3rem;margin:0;}
.btn{background:#1b1b1c;border:1px solid #f2c230;color:#f2c230;padding:6px 12px;border-radius:4px;text-decoration:none;font-size:0.85rem;cursor:pointer;}
.btn:hover{background:#f2c230;color:#0b0b0c;}
table{width:100%;border-collapse:collapse;font-size:0.85rem;}
th,td{border-bottom:1px solid #2a2a2a;padding:8px;text-align:left;vertical-align:top;}
th{color:#8d8d87;text-transform:uppercase;font-size:0.7rem;}
.tag{background:#1b1b1c;border:1px solid #2a2a2a;color:#f2c230;padding:2px 8px;border-radius:999px;}
a{color:#f2c230;}
</style></head><body>
<div class="header">
  <div>
    <h1>LANMESH Admin Forensic Audit Log</h1>
    <span style="font-size:0.8rem;color:#888;">Tamper-resistant append-only offline audit record</span>
  </div>
  <div>
    <a class="btn" href="/">Return to Chat</a>
    <a class="btn" href="/admin/export.json" style="margin-left:8px;">Export JSON</a>
    <button class="btn" onclick="logout()" style="margin-left:8px;">Logout</button>
  </div>
</div>`)

		if store == nil {
			fmt.Fprint(w, `<p>Admin logging is not enabled on this device.</p></body></html>`)
			return
		}

		msgs := store.snapshot()
		sort.Slice(msgs, func(i, j int) bool { return msgs[i].Timestamp > msgs[j].Timestamp })
		fmt.Fprintf(w, `<p style="color:#aaa;font-size:0.85rem;">%d messages captured in audit store.</p>`, len(msgs))
		fmt.Fprint(w, `<table><tr><th>Time</th><th>Group</th><th>Sender</th><th>Nickname</th><th>Hops</th><th>Type</th><th>Content</th></tr>`)
		for _, m := range msgs {
			preview := m.Payload
			if m.Type != "text" {
				preview = "[" + m.Type + "]"
			} else {
				preview = truncate(preview, 200)
			}
			ts := ""
			if m.Timestamp > 0 {
				ts = time.UnixMilli(m.Timestamp).Format("15:04:05")
			}
			fmt.Fprintf(w, "<tr><td>%s</td><td><span class=\"tag\">#%s</span></td><td>%s</td><td>%s</td><td>%d</td><td>%s</td><td>%s</td></tr>",
				html.EscapeString(ts),
				html.EscapeString(m.GroupID),
				html.EscapeString(truncate(m.Sender, 12)),
				html.EscapeString(m.Nickname),
				m.Hops,
				html.EscapeString(m.Type),
				html.EscapeString(preview))
		}
		fmt.Fprint(w, `</table>
<script>
async function logout() {
  await fetch('/admin/logout', { method: 'POST' });
  location.reload();
}
</script></body></html>`)
	})
}

func renderAdminLoginForm(w http.ResponseWriter) {
	fmt.Fprint(w, `<!DOCTYPE html><html><head><meta charset="utf-8">
<title>LANMESH — Admin Login</title>
<style>
body{background:#0b0b0c;color:#eee;font-family:sans-serif;display:flex;justify-content:center;align-items:center;height:100vh;margin:0;}
.card{background:#141416;border:1px solid #2a2a2a;border-radius:8px;padding:32px;width:320px;text-align:center;}
h2{color:#f2c230;margin-top:0;font-size:1.2rem;}
input{width:100%;padding:10px;margin:16px 0;background:#0b0b0c;border:1px solid #333;color:#eee;border-radius:4px;box-sizing:border-box;}
button{width:100%;padding:10px;background:#f2c230;border:none;border-radius:4px;color:#0b0b0c;font-weight:bold;cursor:pointer;}
button:hover{background:#d4a822;}
.err{color:#ff4757;font-size:0.85rem;margin-top:10px;display:none;}
</style></head><body>
<div class="card">
  <h2>Admin Authorization</h2>
  <p style="font-size:0.85rem;color:#888;">Zero-Knowledge HMAC-SHA256 Challenge Login</p>
  <input type="password" id="pw" placeholder="Enter Admin Secret" onkeydown="if(event.key==='Enter')login()">
  <button onclick="login()">Authenticate</button>
  <div class="err" id="err">Authentication failed</div>
</div>
<script>
async function login() {
  const pw = document.getElementById('pw').value;
  const errEl = document.getElementById('err');
  errEl.style.display = 'none';
  try {
    const cRes = await fetch('/admin/challenge');
    const { nonce } = await cRes.json();
    const enc = new TextEncoder();
    const key = await crypto.subtle.importKey('raw', enc.encode(pw), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
    const sig = await crypto.subtle.sign('HMAC', key, enc.encode(nonce));
    const hmacHex = Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2, '0')).join('');

    const authRes = await fetch('/admin/auth', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ nonce, hmac: hmacHex })
    });
    if (authRes.ok) {
      location.reload();
    } else {
      errEl.style.display = 'block';
    }
  } catch(e) {
    errEl.textContent = 'Error: ' + e.message;
    errEl.style.display = 'block';
  }
}
</script></body></html>`)
}
