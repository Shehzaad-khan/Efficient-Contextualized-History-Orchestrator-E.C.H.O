/**
 * chrome_tracker.js — Chrome page signals, Chrome Extension Content Script
 * Echo Personal Memory System
 *
 * Injected into every page except youtube.com (see manifest content_scripts).
 * It only MEASURES; background.js owns the intent gate (§6.2), the dwell clock
 * and every network call. Every 5 s it sends CHROME_PAGE_SIGNAL with:
 *   - scroll depth (deepest point reached, 0..1)
 *   - interaction count (clicks on the page + text selections)
 *   - readable text (up to 12,000 chars), only for ordinary pages
 *   - live URL, title, referrer, and whether this is an app page
 * When the page goes away it sends CHROME_PAGE_UNLOAD so background.js can
 * send a page that passed the gate.
 *
 * Text is never read from:
 *   - app pages (Slack, Notion, Jira ... — §6.7): engagement only
 *   - sensitive URLs (login, payment, account ...)
 *   - pages with private form fields (password / email / phone inputs, or 5+
 *     data-entry fields)
 * The app-domain and sensitive-term lists are the third copy of the same lists;
 * they MUST match background.js and ingestion/chrome/ (decisions A7, A9).
 *
 * Incognito: skipped here as a first line of defence; background.js re-checks
 * sender.tab.incognito, which is the authoritative gate.
 * Logging: none. A content script's console is the page's console.
 */

(function () {
  "use strict";

  // chrome.extension is a legacy namespace and is not guaranteed to exist in an
  // MV3 content script; optional chaining keeps a missing namespace from throwing.
  if (chrome.extension?.inIncognitoContext === true) {
    return;
  }

  const SIGNAL_INTERVAL_MS = 5000;
  const MIN_CONTENT_CHARS = 200;
  const MAX_CONTENT_CHARS = 12000;
  const PRIVATE_FORM_FIELD_COUNT = 5;

  const APP_DOMAINS = [
    "slack.com",
    "app.slack.com",
    "jira.com",
    "atlassian.net",
    "notion.so",
    "figma.com",
    "linear.app",
    "trello.com",
    "asana.com",
    "confluence.atlassian.net"
  ];
  const SENSITIVE_PATH_TERMS = new Set([
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

  // Non-data-entry controls. Counting these towards the "looks like a form"
  // heuristic below made ordinary article pages register as private form
  // surfaces — a Wikipedia article has 8 UI checkboxes, 2 search boxes and 2
  // hidden inputs and nothing a user types personal data into, which was
  // enough to suppress content extraction on most of the web.
  const NON_ENTRY_INPUT_TYPES = new Set([
    "button",
    "checkbox",
    "hidden",
    "image",
    "radio",
    "range",
    "reset",
    "search",
    "submit"
  ]);

  let maxScrollDepth = 0;
  let interactionCount = 0;
  let hadSelection = false;
  let intervalId = null;
  // Text extraction clones the whole page, so it runs once per URL, not every
  // 5 s; an empty result (page still loading) is retried on the next signal.
  let extractedForUrl = null;
  let cachedExtract = "";

  function computeScrollDepth() {
    const documentHeight = Math.max(
      document.body?.scrollHeight || 0,
      document.documentElement?.scrollHeight || 0
    );
    const viewportHeight = window.innerHeight || 0;
    const denominator = Math.max(documentHeight - viewportHeight, 1);
    const depth = Math.min(window.scrollY / denominator, 1);
    maxScrollDepth = Math.max(maxScrollDepth, depth);
  }

  function countWords(text) {
    return (text || "").trim().split(/\s+/).filter(Boolean).length;
  }

  function detectApplicationPage() {
    const host = window.location.hostname.toLowerCase();
    return APP_DOMAINS.some((domain) => host === domain || host.endsWith(`.${domain}`));
  }

  /** Same rule as background.js isSensitiveChromeUrl. */
  function detectSensitivePage() {
    const pathParts = window.location.pathname
      .toLowerCase()
      .split("/")
      .filter(Boolean);
    // location.search already starts with "?" (or is empty).
    const target = `${window.location.pathname}${window.location.search}`.toLowerCase();
    return (
      pathParts.some((part) => SENSITIVE_PATH_TERMS.has(part)) ||
      [...SENSITIVE_PATH_TERMS].some((term) => target.includes(`/${term}`) || target.includes(`${term}=`))
    );
  }

  function isDataEntryField(element) {
    if (element.tagName !== "INPUT") {
      return true;
    }
    return !NON_ENTRY_INPUT_TYPES.has((element.getAttribute("type") || "text").toLowerCase());
  }

  function hasPrivateFormSurface() {
    if (document.querySelector("input[type='password'], input[type='email'], input[type='tel'], input[type='number']")) {
      return true;
    }
    const editableCount = [
      ...document.querySelectorAll("input, textarea, select, [contenteditable=''], [contenteditable='true']")
    ].filter(isDataEntryField).length;
    return editableCount >= PRIVATE_FORM_FIELD_COUNT;
  }

  function visibleTextFrom(node) {
    const clone = node.cloneNode(true);
    clone
      .querySelectorAll("script, style, noscript, input, textarea, select, button, form, [contenteditable=''], [contenteditable='true']")
      .forEach((child) => child.remove());
    return (clone.innerText || clone.textContent || "").replace(/\s+/g, " ").trim();
  }

  function extractReadableContent() {
    if (detectApplicationPage() || detectSensitivePage() || hasPrivateFormSurface()) {
      return "";
    }

    const candidates = [
      document.querySelector("article"),
      document.querySelector("main"),
      document.querySelector("[role='main']"),
      document.body
    ].filter(Boolean);

    for (const node of candidates) {
      const text = visibleTextFrom(node);
      if (text.length >= MIN_CONTENT_CHARS) {
        return text.slice(0, MAX_CONTENT_CHARS);
      }
    }

    return "";
  }

  function currentExtract() {
    const url = window.location.href;
    if (extractedForUrl !== url || cachedExtract === "") {
      cachedExtract = extractReadableContent();
      extractedForUrl = url;
    }
    return cachedExtract;
  }

  function stopSignals() {
    if (intervalId !== null) {
      clearInterval(intervalId);
      intervalId = null;
    }
  }

  function sendSignal(type) {
    try {
      // No runtime id = the extension was reloaded; this old script is orphaned.
      if (!chrome.runtime?.id) {
        stopSignals();
        return;
      }

      const contentExtract = currentExtract();
      chrome.runtime.sendMessage(
        {
          type,
          url: window.location.href,
          title: document.title,
          scrollDepth: Number(maxScrollDepth.toFixed(3)),
          interactionCount,
          contentExtract,
          wordCount: countWords(contentExtract),
          referrer: document.referrer || "",
          isAppPage: detectApplicationPage()
        },
        () => {
          const lastError = chrome.runtime.lastError;
          if (lastError && lastError.message?.includes("context invalidated")) {
            stopSignals();
          }
        }
      );
    } catch (error) {
      // Extension context gone: nothing more can be sent. Any other error
      // (an odd page breaking text extraction) skips only this signal.
      if (!chrome.runtime?.id) {
        stopSignals();
      }
    }
  }

  window.addEventListener("scroll", computeScrollDepth, { passive: true });

  document.addEventListener(
    "click",
    () => {
      interactionCount += 1;
    },
    { passive: true }
  );

  // A drag-select fires selectionchange many times; count one selection once,
  // when it goes from empty to non-empty.
  document.addEventListener("selectionchange", () => {
    const hasSelection = Boolean(document.getSelection()?.toString().trim());
    if (hasSelection && !hadSelection) {
      interactionCount += 1;
    }
    hadSelection = hasSelection;
  });

  computeScrollDepth();
  intervalId = setInterval(() => sendSignal("CHROME_PAGE_SIGNAL"), SIGNAL_INTERVAL_MS);
  // pagehide fires on every unload, including pages kept in the back/forward
  // cache, where beforeunload can be skipped.
  window.addEventListener("pagehide", () => sendSignal("CHROME_PAGE_UNLOAD"));
})();
