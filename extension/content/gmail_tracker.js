/**
 * gmail_tracker.js — GMC Module, Chrome Extension Content Script
 * Echo Personal Memory System
 *
 * Injected into mail.google.com. Detects when the user opens and actually
 * reads an email, then reports foreground reading time to the backend so
 * memory_engagement can be updated.
 *
 * Why a separate tracker (not chrome_tracker.js):
 *   Gmail is an "application domain" — chrome_tracker treats it as a private
 *   app and never extracts content. But the architecture still wants email
 *   OPEN engagement (dwell_time, first_opened_at, session count). That is what
 *   this script provides, scoped to a single opened email.
 *
 * Email id resolution:
 *   The Gmail URL hash uses permalink ids that do NOT match the Gmail API
 *   message id stored in gmail_metadata.email_id. The open message DOM node
 *   carries `data-legacy-message-id`, which IS the hex API id. We read that.
 *
 * "Visited properly" gate (mirrors Chrome timer rules):
 *   Reading time counts ONLY when ALL of these hold:
 *     1. An email is open in the reading pane
 *     2. Tab is in foreground (document.visibilityState === 'visible')
 *     3. Browser window has focus
 *     4. User not idle > 30 seconds (no mouse/keyboard activity)
 *
 *   Passive reading credit:
 *     Long emails are often read with no clicks at all, which would trip the
 *     idle gate above and stop the timer mid-read. So when an email stays in
 *     focus (foreground + window focused) for FOCUS_CREDIT_SECONDS uninterrupted
 *     — even with zero input — one full minute is added to the reading time.
 *
 *   Engagement is held PENDING and only sent once accumulated reading time
 *   crosses MIN_VISIT_SECONDS — an accidental 1-second open is never recorded.
 *
 * Session semantics:
 *   The backend accumulates dwell and bumps play_sessions_count by 1 per call,
 *   so we send exactly ONCE per open-email session (when the user switches
 *   emails or leaves the tab), never on a repeating heartbeat.
 */

// chrome.extension is a legacy namespace and is not guaranteed to exist in an
// MV3 content script; optional chaining keeps a missing namespace from
// throwing and killing the whole tracker. background.js re-checks
// sender.tab.incognito, which is the authoritative gate.
if (chrome.extension?.inIncognitoContext !== true) {
  "use strict";

  const IDLE_THRESHOLD_MS = 30000;   // user considered idle after 30s of no input
  const MIN_VISIT_SECONDS = 3;       // below this, the open was not a "proper" visit
  const FOCUS_CREDIT_SECONDS = 60;   // a full minute of in-focus reading is credited
                                     // even with no clicks (passive reading credit)

  const state = {
    emailId: null,          // legacy (hex API) id of the currently open email
    dwellSeconds: 0,        // foreground reading time accumulated this session
    focusStreakSeconds: 0,  // uninterrupted in-focus seconds while idle (passive read)
    sent: false,            // true once this session's engagement was reported
    isTabForeground: document.visibilityState === "visible",
    isWindowFocused: document.hasFocus(),
    lastActivityTime: Date.now(),
    timerHandle: null,
  };

  // ---------------------------------------------------------------------------
  // Open-email detection
  // ---------------------------------------------------------------------------
  // Returns the hex API message id of the email the user is currently reading,
  // or null if no email is open. In a thread there may be several messages; we
  // pick the visible one nearest the top of the viewport — i.e. what is being
  // read right now.
  function getOpenEmailId() {
    const nodes = document.querySelectorAll("[data-legacy-message-id]");
    let best = null;
    let bestTop = Infinity;

    for (const node of nodes) {
      if (node.offsetParent === null) continue; // not rendered / collapsed
      const rect = node.getBoundingClientRect();
      if (rect.height === 0) continue;
      // Skip messages scrolled fully above the viewport.
      if (rect.bottom <= 0) continue;
      const distanceFromTop = Math.abs(rect.top);
      if (distanceFromTop < bestTop) {
        bestTop = distanceFromTop;
        best = node.getAttribute("data-legacy-message-id");
      }
    }
    return best;
  }

  function isCountingActive() {
    const notIdle = Date.now() - state.lastActivityTime < IDLE_THRESHOLD_MS;
    return state.emailId !== null && state.isTabForeground && state.isWindowFocused && notIdle;
  }

  // ---------------------------------------------------------------------------
  // Reporting
  // ---------------------------------------------------------------------------
  function finalizeSession() {
    // Commit the current email's reading time, if it was a genuine visit.
    if (state.emailId && !state.sent && state.dwellSeconds >= MIN_VISIT_SECONDS) {
      sendEngagement(state.emailId, state.dwellSeconds);
      state.sent = true;
    }
  }

  function startSession(emailId) {
    state.emailId = emailId;
    state.dwellSeconds = 0;
    state.focusStreakSeconds = 0;
    state.sent = false;
  }

  function sendEngagement(emailId, dwellSeconds) {
    try {
      if (!chrome.runtime?.id) return;
      chrome.runtime.sendMessage(
        {
          type: "GMC_ENGAGEMENT",
          payload: {
            email_id: emailId,
            dwell_seconds: dwellSeconds,
          },
        },
        () => void chrome.runtime.lastError // swallow "no receiver" on teardown
      );
    } catch (_error) {
      // Extension context invalidated (reload/update) — nothing to do.
    }
  }

  // ---------------------------------------------------------------------------
  // Tick — runs every second, drives detection + accumulation
  // ---------------------------------------------------------------------------
  function tick() {
    const openId = getOpenEmailId();

    // Email changed (switched message, or closed back to the list).
    if (openId !== state.emailId) {
      finalizeSession();           // commit the email we were reading
      if (openId) {
        startSession(openId);      // begin tracking the new one
      } else {
        state.emailId = null;      // back to inbox list — nothing open
      }
      return;
    }

    if (isCountingActive()) {
      // Active reading — input within the last 30s. Count each second.
      state.dwellSeconds += 1;
      state.focusStreakSeconds = 0;   // active path handles this second
    } else if (state.emailId && state.isTabForeground && state.isWindowFocused) {
      // Idle but the email is still on screen — genuine passive reading.
      // Credit a full minute once it has held focus for FOCUS_CREDIT_SECONDS.
      state.focusStreakSeconds += 1;
      if (state.focusStreakSeconds >= FOCUS_CREDIT_SECONDS) {
        state.dwellSeconds += FOCUS_CREDIT_SECONDS;
        state.focusStreakSeconds = 0;
      }
    } else {
      // Tab/window lost focus — the passive streak breaks.
      state.focusStreakSeconds = 0;
    }
  }

  // ---------------------------------------------------------------------------
  // Foreground / focus / idle signals
  // ---------------------------------------------------------------------------
  document.addEventListener("visibilitychange", () => {
    state.isTabForeground = document.visibilityState === "visible";
  });
  window.addEventListener("focus", () => {
    state.isWindowFocused = true;
  });
  window.addEventListener("blur", () => {
    state.isWindowFocused = false;
  });
  ["mousemove", "keydown", "click", "scroll"].forEach((evt) => {
    document.addEventListener(
      evt,
      () => {
        state.lastActivityTime = Date.now();
      },
      { passive: true }
    );
  });

  // Closing the tab / navigating away — flush the open email.
  window.addEventListener("beforeunload", finalizeSession);

  // ---------------------------------------------------------------------------
  // Start
  // ---------------------------------------------------------------------------
  state.timerHandle = setInterval(tick, 1000);
}
