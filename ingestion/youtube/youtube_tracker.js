/**
 * youtube_tracker.js — YTC Module, Chrome Extension Content Script
 * Echo Personal Memory System
 *
 * Injected into youtube.com/watch* and youtube.com/shorts/* pages.
 *
 * Responsibilities:
 *   - Detect video element and attach playback event listeners
 *   - Track foreground watch time (4-condition timer per architecture)
 *   - Detect manual interactions (pause, seek, speed change)
 *   - Send events to background.js for routing to backend
 *
 * Watch time counts ONLY when ALL four conditions are true:
 *   1. Video is playing (not paused, not ended)
 *   2. Tab is in foreground (document.visibilityState === 'visible')
 *   3. Browser window has focus
 *   4. User not idle > 30 seconds (no mouse/keyboard activity)
 *
 * Intent gate (ANY ONE must pass — enforced on backend too as safety net):
 *   Option A: watch_time_seconds >= 20
 *   Option B: manual interaction detected (pause/seek/speed)
 *   Option C: revisit (checked by backend via Redis)
 */

(function () {
  "use strict";

  // ---------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------
  const state = {
    videoId: null,
    isShort: false,
    watchTimeSeconds: 0,
    isPlaying: false,
    isTabForeground: true,
    isWindowFocused: true,
    lastActivityTime: Date.now(),
    intentFired: false,          // true once we've sent video-detected to backend
    heartbeatInterval: null,
    idleCheckInterval: null,
    manualInteractionDetected: false,
    interactionType: null,
  };

  const IDLE_THRESHOLD_MS = 30000;    // 30 seconds
  const HEARTBEAT_INTERVAL_MS = 5000; // send heartbeat every 5 seconds
  const INTENT_WATCH_THRESHOLD = 20;  // seconds — Option A threshold


  // ---------------------------------------------------------------------------
  // Video ID extraction from current URL
  // ---------------------------------------------------------------------------
  function extractVideoId(url) {
    if (!url) return null;

    if (url.includes("watch?v=")) {
      const id = new URLSearchParams(new URL(url).search).get("v");
      return id && id.length === 11 ? id : null;
    }

    if (url.includes("/shorts/")) {
      const parts = url.split("/shorts/")[1];
      const id = parts ? parts.split("?")[0].split("/")[0] : null;
      return id && id.length === 11 ? id : null;
    }

    return null;
  }

  function isShortUrl(url) {
    return url.includes("/shorts/");
  }


  // ---------------------------------------------------------------------------
  // Timer — counts foreground watch time only
  // ---------------------------------------------------------------------------
  function isCountingActive() {
    const notIdle = (Date.now() - state.lastActivityTime) < IDLE_THRESHOLD_MS;
    return (
      state.isPlaying &&
      state.isTabForeground &&
      state.isWindowFocused &&
      notIdle
    );
  }

  function startWatchTimer() {
    // Tick every 1 second — increment only when all 4 conditions hold
    if (state._watchTimerHandle) return; // already running

    state._watchTimerHandle = setInterval(() => {
      if (isCountingActive()) {
        state.watchTimeSeconds += 1;

        // Check Option A threshold — fire intent if not already fired
        if (!state.intentFired && state.watchTimeSeconds >= INTENT_WATCH_THRESHOLD) {
          fireVideoDetected("watch_time");
        }
      }
    }, 1000);
  }

  function stopWatchTimer() {
    if (state._watchTimerHandle) {
      clearInterval(state._watchTimerHandle);
      state._watchTimerHandle = null;
    }
  }


  // ---------------------------------------------------------------------------
  // Heartbeat — sends cumulative watch_time to backend every 5 seconds
  // Only runs after intent has fired (video is being saved)
  // ---------------------------------------------------------------------------
  function startHeartbeat() {
    if (state.heartbeatInterval) return;

    state.heartbeatInterval = setInterval(() => {
      if (!state.videoId || !state.intentFired) return;

      chrome.runtime.sendMessage({
        type: "YTC_HEARTBEAT",
        payload: {
          video_id: state.videoId,
          watch_time_seconds: state.watchTimeSeconds,
          timestamp: new Date().toISOString(),
        },
      });
    }, HEARTBEAT_INTERVAL_MS);
  }

  function stopHeartbeat() {
    if (state.heartbeatInterval) {
      clearInterval(state.heartbeatInterval);
      state.heartbeatInterval = null;
    }
  }


  // ---------------------------------------------------------------------------
  // Intent gate — send video-detected to backend
  // ---------------------------------------------------------------------------
  function fireVideoDetected(triggeredBy, interactionType = null) {
    if (state.intentFired) return; // already sent — don't duplicate
    if (!state.videoId) return;

    state.intentFired = true;

    chrome.runtime.sendMessage({
      type: "YTC_VIDEO_DETECTED",
      payload: {
        url: window.location.href,
        video_id: state.videoId,
        is_short: state.isShort,
        watch_time_seconds: state.watchTimeSeconds,
        triggered_by: triggeredBy,
        interaction_type: interactionType,
        timestamp: new Date().toISOString(),
      },
    });

    // Start heartbeat now that video is being tracked
    startHeartbeat();
  }

  function fireVideoClosed() {
    if (!state.videoId || !state.intentFired) return;

    stopHeartbeat();

    chrome.runtime.sendMessage({
      type: "YTC_VIDEO_CLOSED",
      payload: {
        video_id: state.videoId,
        final_watch_time_seconds: state.watchTimeSeconds,
        timestamp: new Date().toISOString(),
      },
    });
  }


  // ---------------------------------------------------------------------------
  // Video element event listeners
  // ---------------------------------------------------------------------------
  function attachVideoListeners(video) {
    video.addEventListener("play", () => {
      state.isPlaying = true;
      startWatchTimer();
    });

    video.addEventListener("pause", () => {
      state.isPlaying = false;

      // Option B — manual pause (not autoplay end)
      if (!video.ended && !state.intentFired) {
        state.manualInteractionDetected = true;
        state.interactionType = "pause";
        fireVideoDetected("manual_interaction", "pause");
      }
    });

    video.addEventListener("ended", () => {
      state.isPlaying = false;
      stopWatchTimer();
    });

    video.addEventListener("seeked", () => {
      // Option B — user seeked to a timestamp
      if (!state.intentFired) {
        fireVideoDetected("manual_interaction", "seek");
      }
    });

    video.addEventListener("ratechange", () => {
      // Option B — user changed playback speed
      if (!state.intentFired && video.playbackRate !== 1) {
        fireVideoDetected("manual_interaction", "speed_change");
      }
    });
  }


  // ---------------------------------------------------------------------------
  // Visibility and focus tracking
  // ---------------------------------------------------------------------------
  document.addEventListener("visibilitychange", () => {
    state.isTabForeground = document.visibilityState === "visible";
    // Timer continues running — isCountingActive() handles the gate
  });

  window.addEventListener("focus", () => {
    state.isWindowFocused = true;
  });

  window.addEventListener("blur", () => {
    state.isWindowFocused = false;
  });

  // Idle detection — reset on any user activity
  ["mousemove", "keydown", "click", "scroll"].forEach((evt) => {
    document.addEventListener(evt, () => {
      state.lastActivityTime = Date.now();
    }, { passive: true });
  });


  // ---------------------------------------------------------------------------
  // Page unload — fire video-closed to finalize watch time
  // ---------------------------------------------------------------------------
  window.addEventListener("beforeunload", () => {
    fireVideoClosed();
  });

  // YouTube is a SPA — URL changes without full page reload
  // Use a MutationObserver on the title to detect navigation
  let lastUrl = window.location.href;

  new MutationObserver(() => {
    const currentUrl = window.location.href;
    if (currentUrl !== lastUrl) {
      // User navigated to a new video — finalize previous
      fireVideoClosed();
      lastUrl = currentUrl;
      resetState();
      init();
    }
  }).observe(document.querySelector("title"), { childList: true });


  // ---------------------------------------------------------------------------
  // State reset between video navigations
  // ---------------------------------------------------------------------------
  function resetState() {
    stopWatchTimer();
    stopHeartbeat();

    state.videoId = null;
    state.isShort = false;
    state.watchTimeSeconds = 0;
    state.isPlaying = false;
    state.intentFired = false;
    state.manualInteractionDetected = false;
    state.interactionType = null;
    state.lastActivityTime = Date.now();
  }


  // ---------------------------------------------------------------------------
  // Init — called on page load and on SPA navigation
  // ---------------------------------------------------------------------------
  function init() {
    const url = window.location.href;
    const videoId = extractVideoId(url);

    if (!videoId) return; // not a video page

    state.videoId = videoId;
    state.isShort = isShortUrl(url);

    // Wait for video element to appear in DOM (YouTube loads it dynamically)
    const attachWhenReady = () => {
      const video = document.querySelector("video");
      if (video) {
        attachVideoListeners(video);
      } else {
        // Retry after short delay — YouTube DOM loads async
        setTimeout(attachWhenReady, 500);
      }
    };

    attachWhenReady();
  }

  // ---------------------------------------------------------------------------
  // Start
  // ---------------------------------------------------------------------------
  init();
})();
