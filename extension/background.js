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
const activeChromeTabs = new Map();
let windowFocused = true;
let userIdle = false;
let currentActiveChromeTabId = null;

const CHROME_APPLICATION_DOMAINS = [
  "slack.com",
  "app.slack.com",
  "jira.com",
  "atlassian.net",
  "notion.so",
  "confluence.atlassian.net",
  "figma.com",
  "linear.app",
  "trello.com",
  "asana.com",
  "mail.google.com"
];

const CHROME_APPLICATION_PATH_SNIPPETS = ["github.com/issues", "github.com/pulls"];
const CHROME_IGNORED_QUERY_PARAMS = new Set([
  "utm_source",
  "utm_medium",
  "utm_campaign",
  "utm_term",
  "utm_content",
  "fbclid",
  "gclid",
  "ref",
  "_hsenc",
  "mc_eid",
  "yclid"
]);

chrome.idle.setDetectionInterval(30);

// ---------------------------------------------------------------------------
// Message listener — routes messages from content scripts to backend
// ---------------------------------------------------------------------------
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  const { type, payload } = message;

  if (type === "CHROME_PAGE_SIGNAL" || type === "CHROME_PAGE_UNLOAD") {
    return;
  }

  if (!type || !payload) {
    console.warn("[Echo background] Received malformed message", message);
    return;
  }

  // Route to correct endpoint
  const routes = {
    YTC_VIDEO_DETECTED: "/ytc/video-detected",
    YTC_HEARTBEAT:      "/ytc/heartbeat",
    YTC_VIDEO_CLOSED:   "/ytc/video-closed",
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

function shouldSkipChromeUrl(url, incognito = false) {
  if (!url || incognito) {
    return true;
  }

  if (
    url.startsWith("chrome://") ||
    url.startsWith("chrome-extension://") ||
    url.startsWith("https://www.youtube.com/") ||
    url.startsWith("https://mail.google.com/")
  ) {
    return true;
  }

  try {
    const parsed = new URL(url);
    const host = parsed.hostname.toLowerCase();
    if (CHROME_APPLICATION_DOMAINS.some((domain) => host === domain || host.endsWith(`.${domain}`))) {
      return true;
    }
  } catch (error) {
    return true;
  }

  return CHROME_APPLICATION_PATH_SNIPPETS.some((snippet) => url.includes(snippet));
}

function canonicalizeChromeUrl(rawUrl) {
  try {
    const parsed = new URL(rawUrl);
    for (const key of [...parsed.searchParams.keys()]) {
      if (CHROME_IGNORED_QUERY_PARAMS.has(key.toLowerCase())) {
        parsed.searchParams.delete(key);
      }
    }
    parsed.hash = "";
    return parsed.toString();
  } catch (error) {
    return rawUrl;
  }
}

function ensureChromeTabState(tab) {
  if (!tab || !tab.id || shouldSkipChromeUrl(tab.url, tab.incognito)) {
    return null;
  }

  const canonicalUrl = canonicalizeChromeUrl(tab.url);
  const existing = activeChromeTabs.get(tab.id) || {
    url: tab.url,
    canonicalUrl,
    title: tab.title || "",
    domain: "",
    dwellSeconds: 0,
    scrollDepth: 0,
    interactionCount: 0,
    phase1Passed: false,
    phase2Passed: false,
    revisitSignal: false,
    revisitCount: 0,
    sentToBackend: false
  };

  try {
    existing.domain = new URL(canonicalUrl).hostname.toLowerCase();
  } catch (error) {
    existing.domain = "";
  }

  existing.url = tab.url;
  existing.canonicalUrl = canonicalUrl;
  existing.title = tab.title || existing.title || canonicalUrl;
  activeChromeTabs.set(tab.id, existing);
  return existing;
}

async function checkChromeRevisitSignal(tabState) {
  const response = await postToBackend("/chrome/revisit-check", {
    canonical_url: tabState.canonicalUrl
  });
  tabState.revisitSignal = Boolean(response.is_revisit);
  tabState.revisitCount = tabState.revisitSignal ? 1 : 0;
}

async function sendChromePageToBackend(tabState) {
  if (tabState.sentToBackend || !tabState.phase2Passed) {
    return;
  }

  await postToBackend("/chrome/ingest", {
    url: tabState.url,
    canonical_url: tabState.canonicalUrl,
    title: tabState.title,
    domain: tabState.domain,
    dwell_seconds: tabState.dwellSeconds,
    scroll_depth: tabState.scrollDepth,
    interaction_count: tabState.interactionCount,
    revisit_count: tabState.revisitCount
  });

  tabState.sentToBackend = true;
}

async function evaluateChromeIntent(tabId) {
  const tabState = activeChromeTabs.get(tabId);
  if (!tabState || tabState.sentToBackend) {
    return;
  }

  if (!tabState.phase1Passed && tabState.dwellSeconds >= 5) {
    tabState.phase1Passed = true;
    try {
      await checkChromeRevisitSignal(tabState);
    } catch (error) {
      console.warn("[Echo background] Chrome revisit check failed:", error);
    }
  }

  if (tabState.phase1Passed && !tabState.phase2Passed) {
    const phase2 =
      tabState.dwellSeconds >= 10 ||
      tabState.scrollDepth >= 0.25 ||
      tabState.interactionCount >= 1 ||
      tabState.revisitSignal === true;

    if (phase2) {
      tabState.phase2Passed = true;
      try {
        await sendChromePageToBackend(tabState);
      } catch (error) {
        console.error("[Echo background] Chrome ingest failed:", error);
      }
    }
  }
}

async function finalizeChromeTab(tabId) {
  const tabState = activeChromeTabs.get(tabId);
  if (!tabState) {
    return;
  }
  try {
    await evaluateChromeIntent(tabId);
    await sendChromePageToBackend(tabState);
  } finally {
    activeChromeTabs.delete(tabId);
  }
}

setInterval(async () => {
  if (!windowFocused || userIdle || currentActiveChromeTabId === null) {
    return;
  }

  const tabState = activeChromeTabs.get(currentActiveChromeTabId);
  if (!tabState) {
    return;
  }

  tabState.dwellSeconds += 1;
  await evaluateChromeIntent(currentActiveChromeTabId);
}, 1000);

chrome.tabs.onActivated.addListener(async ({ tabId }) => {
  currentActiveChromeTabId = tabId;
  const tab = await chrome.tabs.get(tabId);
  ensureChromeTabState(tab);
});

chrome.windows.onFocusChanged.addListener((windowId) => {
  windowFocused = windowId !== chrome.windows.WINDOW_ID_NONE;
});

chrome.idle.onStateChanged.addListener((state) => {
  userIdle = state === "idle" || state === "locked";
});

chrome.tabs.onRemoved.addListener((tabId) => {
  finalizeChromeTab(tabId);
  if (currentActiveChromeTabId === tabId) {
    currentActiveChromeTabId = null;
  }
});

chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  if (changeInfo.status === "complete") {
    ensureChromeTabState(tab);
    if (tab.active) {
      currentActiveChromeTabId = tabId;
    }
  }
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type !== "CHROME_PAGE_SIGNAL" && message.type !== "CHROME_PAGE_UNLOAD") {
    return;
  }

  const tabId = sender.tab && sender.tab.id;
  if (!tabId) {
    sendResponse?.({ ok: false, error: "missing_tab_id" });
    return false;
  }

  const tabState = activeChromeTabs.get(tabId) || ensureChromeTabState(sender.tab);
  if (!tabState) {
    sendResponse?.({ ok: false, error: "tab_skipped" });
    return false;
  }

  if (message.type === "CHROME_PAGE_SIGNAL") {
    tabState.url = message.url || tabState.url;
    tabState.canonicalUrl = canonicalizeChromeUrl(tabState.url);
    tabState.title = message.title || tabState.title;
    tabState.scrollDepth = Math.max(tabState.scrollDepth, Number(message.scrollDepth || 0));
    tabState.interactionCount = Math.max(tabState.interactionCount, Number(message.interactionCount || 0));
    sendResponse?.({ ok: true });
  }

  if (message.type === "CHROME_PAGE_UNLOAD") {
    finalizeChromeTab(tabId)
      .then(() => sendResponse?.({ ok: true }))
      .catch((err) => sendResponse?.({ ok: false, error: err.message }));
    return true;
  }

  return false;
});
