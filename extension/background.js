/**
 * background.js - Echo Chrome Extension (MV3 service worker)
 *
 * The only place where captured data leaves the extension. Two jobs:
 *
 * 1. Relay for the YouTube and Gmail content scripts.
 *    YTC_VIDEO_DETECTED / YTC_HEARTBEAT / YTC_VIDEO_CLOSED -> /ytc/...
 *    GMC_ENGAGEMENT                                        -> /gmail/engagement
 *    Their intent gates run on the server; this file only forwards.
 *
 * 2. The Chrome page intent gate (architecture §6.2), run here per tab:
 *    Phase 1: tab in the foreground >= 5 s -> ask /chrome/revisit-check
 *    Phase 2: one of >= 10 s, scroll >= 15 %, >= 1 interaction, or a revisit
 *    Pass -> one POST /chrome/ingest per page. The thresholds MUST match
 *    ingestion/chrome/intent_filter.py, which re-checks every page. The
 *    app-domain and sensitive-term lists are not kept here: they come from
 *    GET /chrome/rules (A9.5, see "Capture rules" below).
 *    Dwell time is counted here (1 s ticks, active tab of the focused window,
 *    user not idle). chrome_tracker.js adds scroll depth, interactions and the
 *    readable text through CHROME_PAGE_SIGNAL every 5 s, and
 *    CHROME_PAGE_UNLOAD when the page goes away.
 *
 * Privacy rules enforced here:
 *    - Every request goes through postToBackend(), which checks the pause flag
 *      (chrome.storage.local.echo_tracking_paused) before each send.
 *    - Incognito tabs are never captured (§13); this is the authoritative check.
 *    - Only http(s) pages; sensitive pages (login, payment, account...) are skipped.
 *    - App pages (Slack, Notion, Jira...) send engagement only, never text (§6.7).
 *    - console output carries endpoints and status codes only, never URLs,
 *      titles or page text (anyone can open the service-worker console).
 *
 * MV3 lifecycle: Chrome stops an idle service worker and starts it again on
 * the next event, so all state below lives in memory and can be lost. The
 * pause flag is therefore read from storage on every send, and the active tab
 * is looked up again at start-up. A page whose worker restarted mid-visit
 * starts counting its dwell time from zero.
 */

const BACKEND_URL = "http://localhost:8000";
const PAUSE_KEY = "echo_tracking_paused";

// §6.2 thresholds — keep equal to ingestion/chrome/intent_filter.py.
const PHASE1_MIN_SECONDS = 5;
const PHASE2_MIN_SECONDS = 10;
const PHASE2_MIN_SCROLL = 0.15;
const PHASE2_MIN_INTERACTIONS = 1;
const APP_PAGE_MIN_SECONDS = 5;

// After a failed /chrome/ingest, wait this long before the next try, so a
// stopped backend is not hit every second. Tab close / unload always tries once more.
const SEND_RETRY_DELAY_MS = 30000;

// tabId -> state of the page currently open in that tab.
const activeChromeTabs = new Map();
let windowFocused = true;
let userIdle = false;
let currentActiveChromeTabId = null;

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

// Content-script message type -> backend route (job 1).
const RELAY_ROUTES = {
  YTC_VIDEO_DETECTED: "/ytc/video-detected",
  YTC_REVISIT_CHECK: "/ytc/revisit-check",
  YTC_HEARTBEAT: "/ytc/heartbeat",
  YTC_VIDEO_CLOSED: "/ytc/video-closed",
  GMC_ENGAGEMENT: "/gmail/engagement"
};

chrome.idle.setDetectionInterval(30);

// ── Backend egress ───────────────────────────────────────────────────────────

async function isTrackingPaused() {
  // Read on every call, not cached: after a worker restart a cached value
  // would be `false` until storage answered, and events could leak while paused.
  const stored = await chrome.storage.local.get({ [PAUSE_KEY]: false });
  return Boolean(stored[PAUSE_KEY]);
}

/** The single exit point for data to the backend. Keep every new network path behind it.
 *  `method` "GET" (the capture rules, no body) is the only other kind so far. */
async function postToBackend(endpoint, payload, method = "POST") {
  if (await isTrackingPaused()) {
    return { paused: true };
  }

  const options =
    method === "GET"
      ? { method }
      : { method, headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) };
  const response = await fetch(`${BACKEND_URL}${endpoint}`, options);

  if (!response.ok) {
    throw new Error(`Backend returned ${response.status} for ${endpoint}`);
  }

  return response.json();
}

// ── Capture rules (A9.5) ─────────────────────────────────────────────────────
// The app-domain and sensitive-word lists live only on the server. They are
// fetched from GET /chrome/rules into chrome.storage.local (chrome_tracker.js
// reads them from there). There is no built-in copy: until the rules are known
// every Chrome page is skipped, so nothing is captured with a missing list.

const RULES_KEY = "echo_chrome_rules";
const RULES_RETRY_DELAY_MS = 30000;
let chromeRules = null; // parsed rules; null = not loaded
let rulesLoading = null;
let rulesNextFetchAt = 0;

function parseChromeRules(raw) {
  if (!raw || typeof raw !== "object") {
    return null;
  }
  const list = (value) =>
    Array.isArray(value) && value.every((item) => typeof item === "string")
      ? value.map((item) => item.toLowerCase())
      : null;
  const appDomains = list(raw.app_domains);
  const publicPaths = list(raw.public_paths);
  const publicSubdomains = list(raw.public_subdomains);
  const exceptions = list(raw.public_subdomain_exceptions);
  const sensitiveTerms = list(raw.sensitive_terms);
  // An empty sensitive list would switch the check off, so it is invalid.
  if (!appDomains || !publicPaths || !publicSubdomains || !exceptions || !sensitiveTerms || sensitiveTerms.length === 0) {
    return null;
  }
  return {
    appDomains,
    publicPaths: new Set(publicPaths),
    publicSubdomains: new Set(publicSubdomains),
    publicSubdomainExceptions: new Set(exceptions),
    sensitiveTerms: new Set(sensitiveTerms)
  };
}

/** Ask the server for the rules and keep them. A failed or invalid answer keeps
 *  what is already known. Goes through postToBackend, so a paused extension
 *  fetches nothing. */
async function fetchChromeRules() {
  rulesNextFetchAt = Date.now() + RULES_RETRY_DELAY_MS;
  try {
    const raw = await postToBackend("/chrome/rules", null, "GET");
    if (raw.paused) {
      return;
    }
    const parsed = parseChromeRules(raw);
    if (!parsed) {
      throw new Error("invalid rules answer");
    }
    chromeRules = parsed;
    await chrome.storage.local.set({ [RULES_KEY]: raw });
  } catch (error) {
    console.warn("[Echo background] Chrome rules not fetched:", error.message);
  }
}

/** Resolve once the rules are in memory (or known to be unavailable). Reads
 *  storage first; fetches only when storage is empty, at most once per 30 s. */
function ensureChromeRules() {
  if (chromeRules) {
    return Promise.resolve(chromeRules);
  }
  if (!rulesLoading) {
    rulesLoading = (async () => {
      try {
        const stored = await chrome.storage.local.get({ [RULES_KEY]: null });
        chromeRules = parseChromeRules(stored[RULES_KEY]);
      } catch (error) {
        chromeRules = null; // storage unreadable: stay closed
      }
      if (!chromeRules && Date.now() >= rulesNextFetchAt) {
        await fetchChromeRules();
      }
      return chromeRules;
    })().finally(() => {
      rulesLoading = null;
    });
  }
  return rulesLoading;
}

// A fresh copy once per browser start / install / update. Between those, the
// stored copy is used.
chrome.runtime.onStartup.addListener(fetchChromeRules);
chrome.runtime.onInstalled.addListener(fetchChromeRules);

// ── URL checks ───────────────────────────────────────────────────────────────

function shouldSkipChromeUrl(url, incognito = false) {
  if (!url || incognito) {
    return true;
  }

  // No rules yet: skip everything rather than capture with a missing list (A9.5).
  if (!chromeRules) {
    return true;
  }

  // Only web pages. This also drops chrome://, chrome-extension://, about:,
  // and file:// (local files can be private documents).
  if (!url.startsWith("http://") && !url.startsWith("https://")) {
    return true;
  }

  try {
    const hostname = new URL(url).hostname.toLowerCase();
    if (hostname === "mail.google.com" || hostname.endsWith(".mail.google.com")) {
      return true;
    }
  } catch (error) {
    return true;
  }

  // YouTube has its own tracker (youtube_tracker.js).
  if (url.startsWith("https://www.youtube.com/")) {
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

/** A private app page (A9.3): an app domain, minus its public sections (help,
 *  blog, docs ... as first path piece; help., docs. ... as first host label).
 *  Same rule as intent_filter.is_application_page and chrome_tracker.js. */
function isAppPageUrl(url) {
  if (!chromeRules) {
    return false;
  }
  try {
    const { hostname, pathname } = new URL(url);
    const host = hostname.toLowerCase();
    const matched = chromeRules.appDomains.find((domain) => host === domain || host.endsWith(`.${domain}`));
    if (!matched) {
      return false;
    }
    if (
      host !== matched &&
      !chromeRules.publicSubdomainExceptions.has(matched) &&
      chromeRules.publicSubdomains.has(host.split(".")[0])
    ) {
      return false;
    }
    const firstPiece = pathname.replace(/^\/+/, "").split("/")[0].toLowerCase();
    return !chromeRules.publicPaths.has(firstPiece);
  } catch (error) {
    return false;
  }
}

/** Whole-token sensitive-page check with the 38-word list (A7.1 + A7.2).
 *  Split host + path on / - _ .; match words against the pieces only.
 *  Also checks path segments with separators removed (e.g. 'sign-in' -> 'signin')
 *  and query keys. */
function isSensitiveChromeUrl(url) {
  if (!chromeRules) {
    return true;
  }
  const CHROME_SENSITIVE_PATH_TERMS = chromeRules.sensitiveTerms;
  try {
    const parsed = new URL(url);
    const target = `${parsed.hostname}${parsed.pathname}`.toLowerCase();
    const tokens = target.split(/[/_\-.]+/).filter(Boolean);
    if (tokens.some((token) => CHROME_SENSITIVE_PATH_TERMS.has(token))) {
      return true;
    }

    const pathSegments = parsed.pathname.toLowerCase().split("/").filter(Boolean);
    for (const seg of pathSegments) {
      const collapsed = seg.replace(/[-_.]+/g, "");
      if (CHROME_SENSITIVE_PATH_TERMS.has(collapsed)) {
        return true;
      }
    }

    for (const key of parsed.searchParams.keys()) {
      const lowerKey = key.toLowerCase();
      if (CHROME_SENSITIVE_PATH_TERMS.has(lowerKey)) {
        return true;
      }
      const keyTokens = lowerKey.split(/[/_\-.]+/).filter(Boolean);
      if (keyTokens.some((token) => CHROME_SENSITIVE_PATH_TERMS.has(token))) {
        return true;
      }
    }

    return false;
  } catch (error) {
    return true;
  }
}

// ── Per-tab page state ───────────────────────────────────────────────────────

function newChromePageState(url, canonicalUrl, title) {
  let domain = "";
  try {
    domain = new URL(canonicalUrl).hostname.toLowerCase();
  } catch (error) {
    domain = "";
  }

  return {
    url,
    canonicalUrl,
    title: title || canonicalUrl,
    domain,
    dwellSeconds: 0,
    scrollDepth: 0,
    interactionCount: 0,
    phase1Passed: false,
    phase2Passed: false,
    revisitSignal: false,
    revisitCount: 0,
    sentToBackend: false,
    sentDwellSeconds: 0,
    sendInFlight: false,
    nextSendAttemptAt: 0,
    contentExtract: "",
    wordCount: 0,
    referrer: "",
    isAppPage: isAppPageUrl(canonicalUrl)
  };
}

/**
 * Return the state for the page now open in `tab`, creating it when needed.
 * When the tab has moved to a different page (normal or single-page-app
 * navigation), the old page is finalised first and the new one starts from
 * zero, so it never inherits the old page's dwell time or "already sent" flag.
 * Returns null (and finalises any old page) when the new URL must be skipped.
 */
function ensureChromeTabState(tab) {
  if (!tab || !tab.id) {
    return null;
  }

  const existing = activeChromeTabs.get(tab.id);
  if (shouldSkipChromeUrl(tab.url, tab.incognito)) {
    if (existing) {
      retireChromeTab(tab.id);
    }
    return null;
  }

  const canonicalUrl = canonicalizeChromeUrl(tab.url);
  if (existing && existing.canonicalUrl === canonicalUrl) {
    existing.url = tab.url;
    existing.title = tab.title || existing.title;
    return existing;
  }

  if (existing) {
    retireChromeTab(tab.id);
  }
  const state = newChromePageState(tab.url, canonicalUrl, tab.title);
  activeChromeTabs.set(tab.id, state);
  return state;
}

// ── Intent gate and sending ──────────────────────────────────────────────────

async function checkChromeRevisitSignal(tabState) {
  const response = await postToBackend("/chrome/revisit-check", {
    canonical_url: tabState.canonicalUrl
  });
  tabState.revisitSignal = Boolean(response.is_revisit);
  tabState.revisitCount = tabState.revisitSignal ? 1 : 0;
}

/**
 * POST the page once. The in-flight flag stops the 1 s ticker from sending a
 * second copy while the first request is still running. A failure leaves
 * sentToBackend false, so a later tick (after the retry delay) or the final
 * flush on close tries again; the error goes back to the caller.
 */
async function sendChromePageToBackend(tabState) {
  if (tabState.sentToBackend || tabState.sendInFlight || !tabState.phase2Passed) {
    return;
  }

  tabState.sendInFlight = true;
  try {
    // When paused, postToBackend sends nothing; the page still counts as
    // handled, so it is not sent later after a resume.
    await postToBackend("/chrome/ingest", {
      url: tabState.url,
      canonical_url: tabState.canonicalUrl,
      title: tabState.title,
      domain: tabState.domain,
      dwell_seconds: tabState.dwellSeconds,
      scroll_depth: tabState.scrollDepth,
      interaction_count: tabState.interactionCount,
      revisit_count: tabState.revisitCount,
      content_extract: tabState.isAppPage ? "" : tabState.contentExtract,
      word_count: tabState.isAppPage ? null : tabState.wordCount,
      referrer: tabState.referrer,
      is_app_page: tabState.isAppPage
    });
    tabState.sentToBackend = true;
    tabState.sentDwellSeconds = tabState.dwellSeconds;
  } catch (error) {
    tabState.nextSendAttemptAt = Date.now() + SEND_RETRY_DELAY_MS;
    throw error;
  } finally {
    tabState.sendInFlight = false;
  }
}

/** Advance the two-phase gate for one page (§6.2). */
async function evaluateChromeIntent(tabState) {
  if (tabState.sentToBackend) {
    return;
  }

  if (!tabState.phase1Passed && tabState.dwellSeconds >= PHASE1_MIN_SECONDS) {
    tabState.phase1Passed = true;
    try {
      await checkChromeRevisitSignal(tabState);
    } catch (error) {
      // Without the answer the page simply has no revisit signal.
      console.warn("[Echo background] Chrome revisit check failed:", error.message);
    }
  }

  if (tabState.isAppPage) {
    // App pages skip the content-based gate: time or one interaction is enough.
    tabState.phase2Passed =
      tabState.dwellSeconds >= APP_PAGE_MIN_SECONDS || tabState.interactionCount >= PHASE2_MIN_INTERACTIONS;
  } else if (tabState.phase1Passed && !tabState.phase2Passed) {
    tabState.phase2Passed =
      tabState.dwellSeconds >= PHASE2_MIN_SECONDS ||
      tabState.scrollDepth >= PHASE2_MIN_SCROLL ||
      tabState.interactionCount >= PHASE2_MIN_INTERACTIONS ||
      tabState.revisitSignal === true;
  }
}

/**
 * Send additional dwell time accumulated since the page was first ingested (C5).
 * Called on tab close or page retirement.
 */
async function sendExtraDwellToBackend(tabState) {
  if (!tabState.sentToBackend || tabState.sendInFlight) {
    return;
  }
  const extraDwell = tabState.dwellSeconds - (tabState.sentDwellSeconds || 0);
  if (extraDwell <= 0) {
    return;
  }

  tabState.sendInFlight = true;
  try {
    await postToBackend("/chrome/ingest", {
      url: tabState.url,
      canonical_url: tabState.canonicalUrl,
      title: tabState.title,
      domain: tabState.domain,
      dwell_seconds: extraDwell,
      scroll_depth: tabState.scrollDepth,
      interaction_count: tabState.interactionCount,
      revisit_count: 0, // the server ADDS revisit_count on conflict; the first send already counted it
      content_extract: tabState.isAppPage ? "" : tabState.contentExtract,
      word_count: tabState.isAppPage ? null : tabState.wordCount,
      referrer: tabState.referrer,
      is_app_page: tabState.isAppPage
    });
    tabState.sentDwellSeconds = tabState.dwellSeconds;
  } catch (error) {
    console.error("[Echo background] Failed to send extra dwell on page close:", error.message);
  } finally {
    tabState.sendInFlight = false;
  }
}

/** Last chance for a page that is going away: send extra dwell if already sent (C5), else evaluate and send. */
async function finalizeChromePage(tabState) {
  try {
    if (tabState.sentToBackend) {
      await sendExtraDwellToBackend(tabState);
    } else {
      await evaluateChromeIntent(tabState);
      await sendChromePageToBackend(tabState);
    }
  } catch (error) {
    console.error("[Echo background] Chrome ingest failed on page close:", error.message);
  }
}

/**
 * Remove the tab's page from the map first (synchronously), then finalise it.
 * Removing first means a new page that the same tab opens meanwhile is never
 * deleted by this old page's cleanup.
 */
function retireChromeTab(tabId) {
  const tabState = activeChromeTabs.get(tabId);
  if (!tabState) {
    return Promise.resolve();
  }
  activeChromeTabs.delete(tabId);
  return finalizeChromePage(tabState);
}

// Dwell clock: one tick per second for the active tab of the focused window.
setInterval(async () => {
  if (!windowFocused || userIdle || currentActiveChromeTabId === null) {
    return;
  }

  const tabState = activeChromeTabs.get(currentActiveChromeTabId);
  if (!tabState) {
    return;
  }

  tabState.dwellSeconds += 1;
  await evaluateChromeIntent(tabState);
  if (tabState.phase2Passed && Date.now() >= tabState.nextSendAttemptAt) {
    try {
      await sendChromePageToBackend(tabState);
    } catch (error) {
      console.error("[Echo background] Chrome ingest failed, retrying in 30 s:", error.message);
    }
  }
}, 1000);

// ── Tab, window and idle events ──────────────────────────────────────────────

async function trackActiveTabOfWindow(query) {
  try {
    const [tab] = await chrome.tabs.query({ active: true, ...query });
    // No tab found (e.g. a devtools window): keep the current one rather than
    // overwrite a tab that a newer event has already set.
    if (tab) {
      currentActiveChromeTabId = tab.id;
      await ensureChromeRules();
      ensureChromeTabState(tab);
    }
  } catch (error) {
    console.warn("[Echo background] Active tab lookup failed:", error.message);
  }
}

// Worker (re)start: pick up the tab the user is on right now.
trackActiveTabOfWindow({ lastFocusedWindow: true });

chrome.tabs.onActivated.addListener(async ({ tabId }) => {
  currentActiveChromeTabId = tabId;
  try {
    const tab = await chrome.tabs.get(tabId);
    await ensureChromeRules();
    ensureChromeTabState(tab);
  } catch (error) {
    // The tab closed before we could read it.
    console.warn("[Echo background] Activated tab not readable:", error.message);
  }
});

chrome.windows.onFocusChanged.addListener((windowId) => {
  windowFocused = windowId !== chrome.windows.WINDOW_ID_NONE;
  // Switching windows fires no onActivated, so look up that window's active tab.
  if (windowFocused) {
    trackActiveTabOfWindow({ windowId });
  }
});

chrome.idle.onStateChanged.addListener((state) => {
  userIdle = state === "idle" || state === "locked";
});

chrome.tabs.onRemoved.addListener((tabId) => {
  if (currentActiveChromeTabId === tabId) {
    currentActiveChromeTabId = null;
  }
  retireChromeTab(tabId);
});

chrome.tabs.onUpdated.addListener(async (tabId, changeInfo, tab) => {
  // changeInfo.url covers single-page-app navigation, which may never
  // reach status "complete" again.
  if (changeInfo.status === "complete" || changeInfo.url) {
    await ensureChromeRules();
    ensureChromeTabState(tab);
    if (tab.active) {
      currentActiveChromeTabId = tabId;
    }
  }
});

// ── Messages from content scripts ────────────────────────────────────────────

/** CHROME_PAGE_SIGNAL / CHROME_PAGE_UNLOAD from chrome_tracker.js. */
function handleChromePageMessage(message, sender, sendResponse) {
  const tab = sender.tab;
  if (!tab || !tab.id) {
    sendResponse({ ok: false, error: "missing_tab_id" });
    return false;
  }

  if (message.type === "CHROME_PAGE_UNLOAD") {
    // The unload can arrive after onUpdated already opened the next page in
    // this tab; only retire the state if it still belongs to the unloading page.
    const current = activeChromeTabs.get(tab.id);
    if (!current || (message.url && current.canonicalUrl !== canonicalizeChromeUrl(message.url))) {
      sendResponse({ ok: true });
      return false;
    }
    retireChromeTab(tab.id).then(() => sendResponse({ ok: true }));
    return true;
  }

  // The content script reports the live URL, which is newer than tab.url
  // after a single-page-app navigation.
  const tabState = ensureChromeTabState({ ...tab, url: message.url || tab.url, title: message.title || tab.title });
  if (!tabState) {
    sendResponse({ ok: false, error: "tab_skipped" });
    return false;
  }

  tabState.scrollDepth = Math.max(tabState.scrollDepth, Number(message.scrollDepth || 0));
  tabState.interactionCount = Math.max(tabState.interactionCount, Number(message.interactionCount || 0));
  tabState.contentExtract = message.contentExtract || tabState.contentExtract;
  tabState.wordCount = Math.max(tabState.wordCount, Number(message.wordCount || 0));
  tabState.referrer = message.referrer || tabState.referrer;
  tabState.isAppPage = Boolean(message.isAppPage || tabState.isAppPage);
  sendResponse({ ok: true });
  return false;
}

/** YouTube / Gmail events: forward the payload to its backend route. */
function relayToBackend(message, sendResponse) {
  const endpoint = RELAY_ROUTES[message.type];
  if (!endpoint || !message.payload) {
    return false;
  }

  postToBackend(endpoint, message.payload)
    .then((response) => sendResponse({ ok: true, data: response }))
    .catch((err) => sendResponse({ ok: false, error: err.message }));
  return true; // the response is sent asynchronously
}

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (!message || !message.type) {
    return false;
  }

  // Incognito is never captured (architecture §13). This is the authoritative
  // check — the content-script guard is only a first line of defence.
  if (sender.tab && sender.tab.incognito) {
    sendResponse({ ok: false, error: "incognito" });
    return false;
  }

  if (message.type === "CHROME_PAGE_SIGNAL") {
    // A page signal may be what woke the worker: make sure the rules are in
    // memory before the page is judged.
    ensureChromeRules().then(() => handleChromePageMessage(message, sender, sendResponse));
    return true;
  }
  if (message.type === "CHROME_PAGE_UNLOAD") {
    return handleChromePageMessage(message, sender, sendResponse);
  }
  return relayToBackend(message, sendResponse);
});
