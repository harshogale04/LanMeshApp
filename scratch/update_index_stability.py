path = r'c:\Users\adity\OneDrive\Desktop\LanMeshApp\core\static\index.html'

with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update connectWS: Add heartbeat ping & do not terminate calls in ws.onclose
old_ws = """      ws.onopen = () => {
        console.log("[WS] Connected to LanMesh node");
        wsReconnectDelay = 1000;
        if (wsReconnectTimer) { clearTimeout(wsReconnectTimer); wsReconnectTimer = null; }
        if (myPeerId) announceIdentity();
        checkPeerStatus();
        if (document.getElementById('topologyPanel').classList.contains('open')) {
          refreshTopology();
        }
      };

      ws.onmessage = handleWSMessage;

      ws.onerror = (err) => {
        console.warn("[WS] Error:", err);
      };

      ws.onclose = () => {
        console.warn("[WS] Closed, scheduling reconnect");
        if (typeof callState !== 'undefined' && callState !== 'idle') {
          endCall();
        }
        if (typeof inRoomCall !== 'undefined' && inRoomCall) {
          leaveRoom();
        }
        scheduleWSRetry();
      };"""

new_ws = """      ws.onopen = () => {
        console.log("[WS] Connected to LanMesh node");
        wsReconnectDelay = 1000;
        if (wsReconnectTimer) { clearTimeout(wsReconnectTimer); wsReconnectTimer = null; }
        startWsHeartbeat();
        if (myPeerId) announceIdentity();
        checkPeerStatus();
        if (document.getElementById('topologyPanel').classList.contains('open')) {
          refreshTopology();
        }
      };

      ws.onmessage = handleWSMessage;

      ws.onerror = (err) => {
        console.warn("[WS] Error:", err);
      };

      ws.onclose = () => {
        console.warn("[WS] Closed, scheduling reconnect");
        if (wsHeartbeatTimer) { clearInterval(wsHeartbeatTimer); wsHeartbeatTimer = null; }
        // Do NOT drop active P2P calls on local WebSocket reconnect!
        scheduleWSRetry();
      };"""

assert old_ws in content, "old_ws not found"
content = content.replace(old_ws, new_ws, 1)

# Add startWsHeartbeat function right before scheduleWSRetry
target_retry = "    function scheduleWSRetry() {"
replacement_retry = """    let wsHeartbeatTimer = null;
    function startWsHeartbeat() {
      if (wsHeartbeatTimer) clearInterval(wsHeartbeatTimer);
      wsHeartbeatTimer = setInterval(() => {
        if (ws && ws.readyState === WebSocket.OPEN) {
          try { ws.send(JSON.stringify({ type: "ping" })); } catch (e) {}
        }
      }, 10000);
    }

    function scheduleWSRetry() {"""

assert target_retry in content, "target_retry not found"
content = content.replace(target_retry, replacement_retry, 1)

# 2. Add pong check in handleWSMessage
target_ws_msg = """    function handleWSMessage(event) {
      const msg = JSON.parse(event.data);"""

replacement_ws_msg = """    function handleWSMessage(event) {
      const msg = JSON.parse(event.data);
      if (msg.type === "pong") return;"""

assert target_ws_msg in content, "target_ws_msg not found"
content = content.replace(target_ws_msg, replacement_ws_msg, 1)

# 3. 1:1 call ICE candidate buffering and 10-second grace timer
old_1to1 = """    function newPeerConnection() {
      const conn = new RTCPeerConnection(CALL_CONFIG);
      if (localStream) {
        localStream.getTracks().forEach(t => conn.addTrack(t, localStream));
      }
      conn.ontrack = (e) => {
        const remoteVid = document.getElementById('remoteVideo');
        if (remoteVid) remoteVid.srcObject = e.streams[0];
      };
      conn.onicecandidate = (e) => {
        if (e.candidate) sendCallSignal('call-ice', callPeerId, { candidate: e.candidate });
      };
      conn.oniceconnectionstatechange = () => {
        if (conn.iceConnectionState === 'disconnected' || conn.iceConnectionState === 'failed' || conn.iceConnectionState === 'closed') {
          if (callState !== 'idle') endCall();
        }
      };
      return conn;
    }"""

new_1to1 = """    let pending1to1Candidates = [];
    let iceDisconnectTimer = null;

    function newPeerConnection() {
      const conn = new RTCPeerConnection(CALL_CONFIG);
      if (localStream) {
        localStream.getTracks().forEach(t => conn.addTrack(t, localStream));
      }
      conn.ontrack = (e) => {
        const remoteVid = document.getElementById('remoteVideo');
        if (remoteVid) {
          remoteVid.srcObject = e.streams[0];
          remoteVid.style.display = 'block';
          remoteVid.play().catch(() => {});
        }
      };
      conn.onicecandidate = (e) => {
        if (e.candidate && callPeerId) {
          sendCallSignal('call-ice', callPeerId, { candidate: e.candidate });
        }
      };
      conn.oniceconnectionstatechange = () => {
        console.log(`[CALL] ICE connection state: ${conn.iceConnectionState}`);
        if (conn.iceConnectionState === 'connected' || conn.iceConnectionState === 'completed') {
          if (iceDisconnectTimer) {
            clearTimeout(iceDisconnectTimer);
            iceDisconnectTimer = null;
          }
        } else if (conn.iceConnectionState === 'disconnected') {
          // Grace period: allow 10s for wireless packet jitter to recover
          if (!iceDisconnectTimer) {
            iceDisconnectTimer = setTimeout(() => {
              if (conn.iceConnectionState === 'disconnected' || conn.iceConnectionState === 'failed') {
                console.warn("[CALL] Disconnected for >10s, ending call");
                if (callState !== 'idle') endCall();
              }
            }, 10000);
          }
        } else if (conn.iceConnectionState === 'failed' || conn.iceConnectionState === 'closed') {
          if (callState !== 'idle') endCall();
        }
      };
      return conn;
    }"""

assert old_1to1 in content, "old_1to1 not found"
content = content.replace(old_1to1, new_1to1, 1)

# In startCall: reset pending1to1Candidates
old_start_call = """      pc = newPeerConnection();
      const offer = await pc.createOffer();"""

new_start_call = """      pending1to1Candidates = [];
      pc = newPeerConnection();
      const offer = await pc.createOffer();"""

assert old_start_call in content, "old_start_call not found"
content = content.replace(old_start_call, new_start_call, 1)

# In acceptCall: flush buffered candidates
old_accept_call = """      pc = newPeerConnection();
      await pc.setRemoteDescription(new RTCSessionDescription(pendingOffer));
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      sendCallSignal('call-answer', callPeerId, { sdp: answer, mode: callMode, nickname: myNickname || 'Peer', avatar: myAvatar });"""

new_accept_call = """      pc = newPeerConnection();
      await pc.setRemoteDescription(new RTCSessionDescription(pendingOffer));
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      sendCallSignal('call-answer', callPeerId, { sdp: answer, mode: callMode, nickname: myNickname || 'Peer', avatar: myAvatar });

      // Flush candidates that arrived while phone was ringing
      if (pending1to1Candidates.length > 0) {
        for (const cand of pending1to1Candidates) {
          try { await pc.addIceCandidate(new RTCIceCandidate(cand)); } catch (e) {}
        }
        pending1to1Candidates = [];
      }"""

assert old_accept_call in content, "old_accept_call not found"
content = content.replace(old_accept_call, new_accept_call, 1)

# In handleCallAnswer: flush buffered candidates
old_call_answer = """      await pc.setRemoteDescription(new RTCSessionDescription(body.sdp));
      callState = 'active';
      showCallScreen('active', `In call with ${peerDisplayName(callPeerId)}`);"""

new_call_answer = """      await pc.setRemoteDescription(new RTCSessionDescription(body.sdp));
      callState = 'active';
      showCallScreen('active', `In call with ${peerDisplayName(callPeerId)}`);

      // Flush candidates that arrived before answer completed
      if (pending1to1Candidates.length > 0) {
        for (const cand of pending1to1Candidates) {
          try { await pc.addIceCandidate(new RTCIceCandidate(cand)); } catch (e) {}
        }
        pending1to1Candidates = [];
      }"""

assert old_call_answer in content, "old_call_answer not found"
content = content.replace(old_call_answer, new_call_answer, 1)

# In handleCallIce: buffer candidates if pc or remoteDescription is not ready
old_call_ice = """    async function handleCallIce(msg) {
      if (!pc) return;
      const body = JSON.parse(msg.payload);
      try { await pc.addIceCandidate(body.candidate); } catch (e) { console.error('ICE add failed:', e); }
    }"""

new_call_ice = """    async function handleCallIce(msg) {
      let body = {};
      try { body = JSON.parse(msg.payload); } catch (e) { return; }
      if (!body.candidate) return;

      if (pc && pc.remoteDescription && pc.remoteDescription.type) {
        try { await pc.addIceCandidate(new RTCIceCandidate(body.candidate)); } catch (e) { console.error('ICE add failed:', e); }
      } else {
        pending1to1Candidates.push(body.candidate);
      }
    }"""

assert old_call_ice in content, "old_call_ice not found"
content = content.replace(old_call_ice, new_call_ice, 1)

# In endCall: clear disconnect timer and candidates
old_end_call = """    function endCall() {
      clearCallTimer();
      stopCallDurationTimer();"""

new_end_call = """    function endCall() {
      clearCallTimer();
      stopCallDurationTimer();
      if (iceDisconnectTimer) { clearTimeout(iceDisconnectTimer); iceDisconnectTimer = null; }
      pending1to1Candidates = [];"""

assert old_end_call in content, "old_end_call not found"
content = content.replace(old_end_call, new_end_call, 1)

# 4. Group call ICE disconnect grace timer
old_room_ice_state = """      conn.oniceconnectionstatechange = () => {
        if (conn.iceConnectionState === 'disconnected' || conn.iceConnectionState === 'failed' || conn.iceConnectionState === 'closed') {
          removeRoomPeer(peerId);
        }
      };"""

new_room_ice_state = """      conn.oniceconnectionstatechange = () => {
        if (conn.iceConnectionState === 'connected' || conn.iceConnectionState === 'completed') {
          if (peerEntry.iceDisconnectTimer) {
            clearTimeout(peerEntry.iceDisconnectTimer);
            peerEntry.iceDisconnectTimer = null;
          }
        } else if (conn.iceConnectionState === 'disconnected') {
          // Grace period: allow 10s for wireless packet jitter to recover
          if (!peerEntry.iceDisconnectTimer) {
            peerEntry.iceDisconnectTimer = setTimeout(() => {
              if (conn.iceConnectionState === 'disconnected' || conn.iceConnectionState === 'failed') {
                console.warn(`[ROOM] Peer ${peerId} disconnected for >10s, removing`);
                removeRoomPeer(peerId);
              }
            }, 10000);
          }
        } else if (conn.iceConnectionState === 'failed' || conn.iceConnectionState === 'closed') {
          removeRoomPeer(peerId);
        }
      };"""

assert old_room_ice_state in content, "old_room_ice_state not found"
content = content.replace(old_room_ice_state, new_room_ice_state, 1)

# In removeRoomPeer: clear timer
old_remove_peer = """    function removeRoomPeer(peerId) {
      const entry = roomPeers.get(peerId);
      if (!entry) return;
      if (entry.pc) {
        entry.pc.close();
      }"""

new_remove_peer = """    function removeRoomPeer(peerId) {
      const entry = roomPeers.get(peerId);
      if (!entry) return;
      if (entry.iceDisconnectTimer) {
        clearTimeout(entry.iceDisconnectTimer);
        entry.iceDisconnectTimer = null;
      }
      if (entry.pc) {
        entry.pc.close();
      }"""

assert old_remove_peer in content, "old_remove_peer not found"
content = content.replace(old_remove_peer, new_remove_peer, 1)

# 5. Geolocation shareLocation toast & options
old_share_loc = """    function shareLocation() {
      if (!navigator.geolocation) {
        uiAlert("Location Unavailable", "Geolocation is not supported by this browser.");
        return;
      }
      navigator.geolocation.getCurrentPosition((position) => {
        const loc = {
          lat: position.coords.latitude,
          lng: position.coords.longitude,
          accuracy: position.coords.accuracy,
          timestamp: Date.now()
        };
        sendMessage("location", JSON.stringify(loc));
      }, (err) => {
        uiAlert("Location Error", "Could not get location: " + err.message);
      }, { enableHighAccuracy: true, timeout: 10000 });
    }"""

new_share_loc = """    function shareLocation() {
      if (!navigator.geolocation) {
        uiAlert("Location Unavailable", "Geolocation is not supported by this browser.");
        return;
      }
      showToast("Acquiring GPS location…");
      navigator.geolocation.getCurrentPosition((position) => {
        const loc = {
          lat: position.coords.latitude,
          lng: position.coords.longitude,
          accuracy: position.coords.accuracy,
          timestamp: Date.now()
        };
        sendMessage("location", JSON.stringify(loc));
        showToast("Location shared!");
      }, (err) => {
        uiAlert("Location Error", "Could not get location: " + err.message);
      }, { enableHighAccuracy: true, timeout: 15000, maximumAge: 10000 });
    }"""

assert old_share_loc in content, "old_share_loc not found"
content = content.replace(old_share_loc, new_share_loc, 1)

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)

print("index.html updated successfully with call stability & geolocation improvements!")
