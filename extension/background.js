/**
 * background.js - Echo Chrome Extension
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
const CHROME_SENSITIVE_PATH_TERMS = new Set([
  "account",
  "auth",
  "billing",
  "callback",
  "checkout",
  "login",
  "logout",
  "oauth",
  "password",
  "payment",
  "profile",
  "settings",
  "signin",
  "signup"
]);
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

// Pause/resume tracking (popup toggle). While paused, no event leaves the
// extension — every backend call funnels through postToBackend below.
let trackingPaused = false;

chrome.storage.local.get({ echo_tracking_paused: false }, (stored) => {
  trackingPaused = Boolean(stored.echo_tracking_paused);
});

chrome.storage.onChanged.addListener((changes, area) => {
  if (area === "local" && changes.echo_tracking_paused) {
    trackingPaused = Boolean(changes.echo_tracking_paused.newValue);
  }
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  const { type, payload } = message;

  if (type === "CHROME_PAGE_SIGNAL" || type === "CHROME_PAGE_UNLOAD") {
    return;
  }

  // Incognito is never captured (architecture §13). This is the authoritative
  // check — the content-script guard is only a first line of defence.
  if (sender.tab && sender.tab.incognito) {
    return;
  }

  if (!type || !payload) {
    return;
  }

  const routes = {
    YTC_VIDEO_DETECTED: "/ytc/video-detected",
    YTC_HEARTBEAT: "/ytc/heartbeat",
    YTC_VIDEO_CLOSED: "/ytc/video-closed",
    GMC_ENGAGEMENT: "/gmail/engagement"
  };

  const endpoint = routes[type];
  if (!endpoint) {
    return;
  }

  postToBackend(endpoint, payload)
    .then((response) => sendResponse({ ok: true, data: response }))
    .catch((err) => sendResponse({ ok: false, error: err.message }));

  return true;
});

async function postToBackend(endpoint, payload) {
  if (trackingPaused) {
    return { paused: true };
  }

  const response = await fetch(`${BACKEND_URL}${endpoint}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
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
    url.startsWith("https://www.youtube.com/")
  ) {
    return true;
  }

  return CHROME_APPLICATION_PATH_SNIPPETS.some((snippet) => url.includes(snippet)) || isSensitiveChromeUrl(url);
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

function isAppDomain(host) {
  return CHROME_APPLICATION_DOMAINS.some((domain) => host === domain || host.endsWith(`.${domain}`));
}

function isSensitiveChromeUrl(url) {
  try {
    const parsed = new URL(url);
    const pathParts = parsed.pathname
      .toLowerCase()
      .split("/")
      .filter(Boolean);
    const target = `${parsed.pathname}?${parsed.searchParams.toString()}`.toLowerCase();
    return (
      pathParts.some((part) => CHROME_SENSITIVE_PATH_TERMS.has(part)) ||
      [...CHROME_SENSITIVE_PATH_TERMS].some((term) => target.includes(`/${term}`) || target.includes(`${term}=`))
    );
  } catch (error) {
    return true;
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
    sentToBackend: false,
    contentExtract: "",
    wordCount: 0,
    referrer: "",
    isAppPage: false,
    isSensitivePage: false
  };

  try {
    existing.domain = new URL(canonicalUrl).hostname.toLowerCase();
  } catch (error) {
    existing.domain = "";
  }

  existing.url = tab.url;
  existing.canonicalUrl = canonicalUrl;
  existing.title = tab.title || existing.title || canonicalUrl;
  existing.isAppPage = isAppDomain(existing.domain);
  existing.isSensitivePage = isSensitiveChromeUrl(canonicalUrl);
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
    revisit_count: tabState.revisitCount,
    content_extract: tabState.isAppPage || tabState.isSensitivePage ? "" : tabState.contentExtract,
    word_count: tabState.isAppPage || tabState.isSensitivePage ? null : tabState.wordCount,
    referrer: tabState.referrer,
    is_app_page: tabState.isAppPage
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

  if (tabState.isAppPage) {
    tabState.phase2Passed = tabState.dwellSeconds >= 5 || tabState.interactionCount >= 1;
  } else if (tabState.phase1Passed && !tabState.phase2Passed) {
    const phase2 =
      tabState.dwellSeconds >= 10 ||
      tabState.scrollDepth >= 0.25 ||
      tabState.interactionCount >= 1 ||
      tabState.revisitSignal === true;

    if (phase2) {
      tabState.phase2Passed = true;
    }
  }

  if (tabState.phase2Passed) {
    try {
      await sendChromePageToBackend(tabState);
    } catch (error) {
      console.error("[Echo background] Chrome ingest failed:", error);
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

  if (sender.tab.incognito) {
    sendResponse?.({ ok: false, error: "incognito" });
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
    tabState.isSensitivePage = isSensitiveChromeUrl(tabState.canonicalUrl);
    tabState.scrollDepth = Math.max(tabState.scrollDepth, Number(message.scrollDepth || 0));
    tabState.interactionCount = Math.max(tabState.interactionCount, Number(message.interactionCount || 0));
    tabState.contentExtract = tabState.isSensitivePage ? "" : message.contentExtract || tabState.contentExtract;
    tabState.wordCount = tabState.isSensitivePage ? 0 : Math.max(tabState.wordCount, Number(message.wordCount || 0));
    tabState.referrer = message.referrer || tabState.referrer;
    tabState.isAppPage = Boolean(message.isAppPage || tabState.isAppPage);
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
