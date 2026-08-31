/**
 * youtube_tracker.js — YTC Module, Chrome Extension Content Script
 * Echo Personal Memory System
 *
 * Injected into every youtube.com page (see manifest content_scripts).
 *
 * Why the whole domain and not just /watch and /shorts:
 *   YouTube is a single-page app. Opening youtube.com and clicking a video is a
 *   history.pushState — no new document, so a content script matched only on
 *   "youtube.com/watch*" is NEVER injected on that navigation. Matching the
 *   whole domain and activating per-URL is the only way to see videos the user
 *   reaches by clicking rather than by pasting a URL.
 *
 * Responsibilities:
 *   - Activate on /watch and /shorts URLs, deactivate on every other page
 *   - Track foreground watch time (4-condition timer per architecture §7.6)
 *   - Detect manual interactions (pause, seek, speed change)
 *   - Send events to background.js for routing to backend
 *
 * Watch time counts ONLY when ALL four conditions are true:
 *   1. Video is playing (not paused, not ended)
 *   2. Tab is in foreground (document.visibilityState === 'visible')
 *   3. Browser window has focus
 *   4. User not idle > 30 seconds
 *      Playback progress counts as activity while conditions 2 and 3 hold —
 *      architecture §7.3's own worked example ("watch OS tutorial 5 minutes →
 *      PASS (300s)") is unreachable if someone sitting still watching a video
 *      is called idle after 30 seconds. Walking away still stops the clock,
 *      via conditions 2 and 3.
 *
 * Intent gate (ANY ONE must pass — enforced on backend too as safety net):
 *   Option A: watch_time_seconds >= 20 (architecture §7.3) or >= 15 (Shorts)
 *   Option B: completion_rate >= 0.5 (watch_time / duration)
 *   Option C: revisit (checked by backend via Redis)
 *   Extra:    manual interaction detected (pause/seek/speed)
 */

(function () {
  "use strict";

  // Flip to true to trace injection / activation / watch-time in the page
  // console — the only way to see inside a content script's isolated world.
  const DEBUG = false;
  const log = (...args) => { if (DEBUG) console.info("[Echo YTC]", ...args); };
  log("injected", location.pathname);

  // ---------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------
  const state = {
    videoId: null,
    isShort: false,
    watchTimeSeconds: 0,
    isPlaying: false,
    isTabForeground: document.visibilityState === "visible",
    isWindowFocused: document.hasFocus(),
    lastActivityTime: Date.now(),
    intentFired: false,          // true once we've sent video-detected to backend
    heartbeatInterval: null,
    manualInteractionDetected: false,
    interactionType: null,
    durationSeconds: 0,          // total video length, from the <video> element
  };

  const IDLE_THRESHOLD_MS = 30000;          // 30 seconds
  const HEARTBEAT_INTERVAL_MS = 5000;       // send heartbeat every 5 seconds
  const TICK_INTERVAL_MS = 1000;            // watch-time + SPA URL check
  const VIDEO_LOOKUP_RETRY_MS = 500;        // YouTube mounts <video> async
  const VIDEO_LOOKUP_MAX_TRIES = 40;        // give up after ~20s on a non-player page
  const INTENT_WATCH_SHORT = 15;            // Option A — Shorts threshold (seconds)
  const INTENT_WATCH_REGULAR = 20;          // Option A — architecture §7.3
  const INTENT_COMPLETION_THRESHOLD = 0.5;  // Option B — completion ratio

  let currentUrl = window.location.href;
  let trackedVideo = null;      // the <video> element we attached listeners to
  let lookupTries = 0;
  let lookupTimer = null;


  // ---------------------------------------------------------------------------
  // Messaging — never throws, extension context can disappear on reload
  // ---------------------------------------------------------------------------
  function send(type, payload) {
    try {
      if (!chrome.runtime?.id) return;
      chrome.runtime.sendMessage({ type, payload }, () => {
        void chrome.runtime.lastError;   // nothing to do; keeps the console clean
      });
    } catch (error) {
      // Extension reloaded/uninstalled mid-session — stop trying.
    }
  }


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

  function evaluateIntent() {
    if (state.intentFired) return;

    // Option A — watch time (Shorts have a lower bar than regular videos)
    const watchThreshold = state.isShort ? INTENT_WATCH_SHORT : INTENT_WATCH_REGULAR;
    if (state.watchTimeSeconds >= watchThreshold) {
      fireVideoDetected("watch_time");
      return;
    }

    // Option B — completion rate (watched at least half the video)
    if (state.durationSeconds > 0) {
      const completionRate = state.watchTimeSeconds / state.durationSeconds;
      if (completionRate >= INTENT_COMPLETION_THRESHOLD) {
        fireVideoDetected("completion_rate");
      }
    }
  }

  // One always-on tick: detects SPA navigation and accumulates watch time.
  setInterval(() => {
    if (window.location.href !== currentUrl) {
      handleUrlChange();
      return;
    }

    if (!state.videoId) return;

    if (isCountingActive()) {
      state.watchTimeSeconds += 1;
      if (state.watchTimeSeconds % 5 === 0) log("watch", state.watchTimeSeconds);
      evaluateIntent();
    }
  }, TICK_INTERVAL_MS);


  // ---------------------------------------------------------------------------
  // Heartbeat — sends cumulative watch_time to backend every 5 seconds
  // Only runs after intent has fired (video is being saved)
  // ---------------------------------------------------------------------------
  function startHeartbeat() {
    if (state.heartbeatInterval) return;

    state.heartbeatInterval = setInterval(() => {
      if (!state.videoId || !state.intentFired) return;

      send("YTC_HEARTBEAT", {
        video_id: state.videoId,
        watch_time_seconds: state.watchTimeSeconds,
        timestamp: new Date().toISOString(),
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
    log("intent fired", triggeredBy, state.watchTimeSeconds);

    send("YTC_VIDEO_DETECTED", {
      url: window.location.href,
      video_id: state.videoId,
      is_short: state.isShort,
      watch_time_seconds: state.watchTimeSeconds,
      triggered_by: triggeredBy,
      interaction_type: interactionType,
      duration_seconds: state.durationSeconds || null,
      timestamp: new Date().toISOString(),
    });

    // Start heartbeat now that video is being tracked
    startHeartbeat();
  }

  function fireVideoClosed() {
    if (!state.videoId || !state.intentFired) return;

    stopHeartbeat();

    send("YTC_VIDEO_CLOSED", {
      video_id: state.videoId,
      final_watch_time_seconds: state.watchTimeSeconds,
      timestamp: new Date().toISOString(),
    });
  }


  // ---------------------------------------------------------------------------
  // Video element event listeners
  // ---------------------------------------------------------------------------
  function readDuration(video) {
    // readyState < HAVE_METADATA means duration still belongs to the previous
    // video on an SPA navigation — using it would let Option B fire against
    // the wrong length.
    if (video.readyState >= 1 && video.duration && isFinite(video.duration)) {
      state.durationSeconds = Math.round(video.duration);
    }
  }

  function attachVideoListeners(video) {
    // YouTube reuses one <video> element across SPA navigations, so listeners
    // must be attached exactly once — otherwise every navigation stacks another
    // copy of every handler on the same element.
    if (trackedVideo === video) {
      readDuration(video);
      return;
    }
    trackedVideo = video;

    readDuration(video);
    video.addEventListener("loadedmetadata", () => readDuration(video));
    video.addEventListener("durationchange", () => readDuration(video));

    video.addEventListener("play", () => {
      state.isPlaying = true;
    });

    video.addEventListener("playing", () => {
      state.isPlaying = true;
    });

    video.addEventListener("pause", () => {
      state.isPlaying = false;

      // Option B — manual pause (not autoplay end)
      if (!video.ended && !state.intentFired && state.videoId) {
        state.manualInteractionDetected = true;
        state.interactionType = "pause";
        fireVideoDetected("manual_interaction", "pause");
      }
    });

    video.addEventListener("ended", () => {
      state.isPlaying = false;
    });

    video.addEventListener("seeked", () => {
      // Option B — user seeked to a timestamp
      if (!state.intentFired && state.videoId) {
        fireVideoDetected("manual_interaction", "seek");
      }
    });

    video.addEventListener("ratechange", () => {
      // Option B — user changed playback speed
      if (!state.intentFired && state.videoId && video.playbackRate !== 1) {
        fireVideoDetected("manual_interaction", "speed_change");
      }
    });

    // Condition 4 — playback progress is engagement (see the header note).
    video.addEventListener("timeupdate", () => {
      if (state.isTabForeground && state.isWindowFocused && !video.paused) {
        state.lastActivityTime = Date.now();
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
  // beforeunload does not fire reliably on mobile-style unloads or bfcache
  // eviction, so pagehide backs it up. fireVideoClosed is idempotent-safe:
  // deactivate() clears videoId, so a second call is a no-op.
  // ---------------------------------------------------------------------------
  window.addEventListener("beforeunload", fireVideoClosed);
  window.addEventListener("pagehide", fireVideoClosed);


  // ---------------------------------------------------------------------------
  // SPA navigation
  //   yt-navigate-finish is YouTube's own navigation event; the 1s URL check in
  //   the tick above is the fallback for when YouTube renames or drops it.
  //   (history.pushState cannot be patched from a content script — the page's
  //   JS runs in a different world.)
  // ---------------------------------------------------------------------------
  function handleUrlChange() {
    const nextUrl = window.location.href;
    if (nextUrl === currentUrl) return;
    currentUrl = nextUrl;
    deactivate();
    activate();
  }

  document.addEventListener("yt-navigate-finish", handleUrlChange);
  window.addEventListener("popstate", handleUrlChange);


  // ---------------------------------------------------------------------------
  // Activate / deactivate for the current URL
  // ---------------------------------------------------------------------------
  function deactivate() {
    fireVideoClosed();
    stopHeartbeat();

    if (lookupTimer) {
      clearTimeout(lookupTimer);
      lookupTimer = null;
    }

    state.videoId = null;
    state.isShort = false;
    state.watchTimeSeconds = 0;
    state.isPlaying = false;
    state.intentFired = false;
    state.manualInteractionDetected = false;
    state.interactionType = null;
    state.durationSeconds = 0;
    state.lastActivityTime = Date.now();
  }

  function activate() {
    const videoId = extractVideoId(window.location.href);
    if (!videoId) return; // home, search, channel page — nothing to track

    log("activate", videoId, { vis: document.visibilityState, focus: document.hasFocus() });
    state.videoId = videoId;
    state.isShort = isShortUrl(window.location.href);
    state.isTabForeground = document.visibilityState === "visible";
    state.isWindowFocused = document.hasFocus();
    state.lastActivityTime = Date.now();
    lookupTries = 0;

    // Wait for the video element to appear in DOM (YouTube loads it async)
    const attachWhenReady = () => {
      lookupTimer = null;
      const video = document.querySelector("video");
      if (video) {
        attachVideoListeners(video);
        // The element is usually already playing by the time we get here —
        // on SPA navigation the "play" event fired before this ran, and on a
        // cold load it fires during document_idle. Seed from the element.
        state.isPlaying = !video.paused && !video.ended;
        return;
      }
      if (++lookupTries < VIDEO_LOOKUP_MAX_TRIES) {
        lookupTimer = setTimeout(attachWhenReady, VIDEO_LOOKUP_RETRY_MS);
      }
    };

    attachWhenReady();
  }

  // ---------------------------------------------------------------------------
  // Start
  // ---------------------------------------------------------------------------
  activate();
})();
