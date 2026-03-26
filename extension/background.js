/**
 * background.js — Echo Chrome Extension
 * Shared service worker for all Echo modules (YTC, CHC, GMC).
 *
 * Responsibilities:
 *   - Route messages from content scripts to the correct backend endpoint
 *   - Handle tab visibility changes (foreground/background detection)
 *   - No module-specific logic lives here — kept intentionally thin
 *
 * Message routing:
 *   { type: 'YTC_VIDEO_DETECTED', payload: {...} }  → /ytc/video-detected
 *   { type: 'YTC_HEARTBEAT',      payload: {...} }  → /ytc/heartbeat
 *   { type: 'YTC_VIDEO_CLOSED',   payload: {...} }  → /ytc/video-closed
 */

const BACKEND_URL = "http://localhost:8000";

// ---------------------------------------------------------------------------
// Message listener — routes messages from content scripts to backend
// ---------------------------------------------------------------------------
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  const { type, payload } = message;

  if (!type || !payload) {
    console.warn("[Echo background] Received malformed message", message);
    return;
  }

  // Route to correct endpoint
  const routes = {
    YTC_VIDEO_DETECTED: "/ytc/video-detected",
    YTC_HEARTBEAT:      "/ytc/heartbeat",
    YTC_VIDEO_CLOSED:   "/ytc/video-closed",
    // CHC routes added here when Chrome module is merged
    // GMC routes added here when Gmail module is merged
  };

  const endpoint = routes[type];
  if (!endpoint) {
    console.warn(`[Echo background] Unknown message type: ${type}`);
    return;
  }

  // Send to backend — fire and forget for heartbeats, await for detections
  postToBackend(endpoint, payload)
    .then((response) => sendResponse({ ok: true, data: response }))
    .catch((err) => {
      console.error(`[Echo background] Backend post failed (${type}):`, err);
      sendResponse({ ok: false, error: err.message });
    });

  // Return true to keep the message channel open for async sendResponse
  return true;
});


// ---------------------------------------------------------------------------
// Backend HTTP helper
// ---------------------------------------------------------------------------
async function postToBackend(endpoint, payload) {
  const response = await fetch(`${BACKEND_URL}${endpoint}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    throw new Error(`Backend returned ${response.status} for ${endpoint}`);
  }

  return response.json();
}
