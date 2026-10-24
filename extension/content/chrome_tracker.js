/**
 * chrome_tracker.js — Chrome page signals, Chrome Extension Content Script
 * Echo Personal Memory System
 *
 * Injected into every page except youtube.com (see manifest content_scripts).
 * It only MEASURES; background.js owns the intent gate (§6.2), the dwell clock
 * and every network call. Every 5 s it sends CHROME_PAGE_SIGNAL with:
 *   - scroll depth (deepest point reached, 0..1)
 *   - interaction count (clicks that are not on a link + text selections)
 *   - readable text (up to 12,000 chars), only for ordinary pages. Starts with
 *     the page's h1-h3 headings, one per line, each prefixed "## " (A13); the
 *     body after them stays one line. ENP lifts the "## " lines into `headings`.
 *   - live URL, title, referrer, and whether this is an app page
 * When the page goes away it sends CHROME_PAGE_UNLOAD so background.js can
 * send a page that passed the gate.
 *
 * Text is never read from:
 *   - app pages (Slack, Notion, Jira ... — §6.7): engagement only
 *   - sensitive URLs (login, payment, account ...)
 *   - pages with private form fields (A7.4), visible fields only: a password
 *     box, 3+ email/phone boxes in one <form>, or 5+ data-entry fields
 * The app-domain and sensitive-term lists are NOT kept here: background.js
 * fetches them from GET /chrome/rules into chrome.storage.local (A9.5) and this
 * script reads them from there. Until they have loaded it extracts no text.
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
  // A13: headings sent as leading "## " lines. MAX_HEADING_CHARS must match
  // enp/text_cleaner.py (a longer "## " line is body text, not a heading).
  const MAX_HEADINGS = 10;
  const MAX_HEADING_CHARS = 200;
  const HEADING_PREFIX = "## ";
  const NON_CONTENT_HEADING_PARENTS =
    "nav, aside, footer, form, [role='navigation'], [role='complementary'], [aria-hidden='true']";
  const PRIVATE_FORM_FIELD_COUNT = 5;
  const PRIVATE_EMAIL_TEL_PER_FORM = 3;

  // Server rules (A9.5), parsed from chrome.storage.local. null = not loaded yet:
  // fail closed, no text is read and no page is called public.
  const RULES_KEY = "echo_chrome_rules";
  let rules = null;

  function parseRules(raw) {
    if (!raw || typeof raw !== "object") return null;
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

  function applyRules(raw) {
    rules = parseRules(raw);
    // A cached empty/old extract must be recomputed under the new rules.
    extractedForUrl = null;
    cachedExtract = "";
  }

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

  /** Same rule as background.js isAppPageUrl and intent_filter.is_application_page
   *  (A9.3): an app domain, minus its public sections. */
  function detectApplicationPage() {
    if (!rules) return false;
    const host = window.location.hostname.toLowerCase();
    const matched = rules.appDomains.find((domain) => host === domain || host.endsWith(`.${domain}`));
    if (!matched) return false;
    if (
      host !== matched &&
      !rules.publicSubdomainExceptions.has(matched) &&
      rules.publicSubdomains.has(host.split(".")[0])
    ) {
      return false;
    }
    const firstPiece = window.location.pathname.replace(/^\/+/, "").split("/")[0].toLowerCase();
    return !rules.publicPaths.has(firstPiece);
  }

  /** Same rule as background.js isSensitiveChromeUrl (A7.1 + A7.2). */
  function detectSensitivePage() {
    if (!rules) return true;
    const SENSITIVE_PATH_TERMS = rules.sensitiveTerms;
    try {
      const target = `${window.location.hostname}${window.location.pathname}`.toLowerCase();
      const tokens = target.split(/[/_\-.]+/).filter(Boolean);
      if (tokens.some((token) => SENSITIVE_PATH_TERMS.has(token))) {
        return true;
      }

      const pathSegments = window.location.pathname.toLowerCase().split("/").filter(Boolean);
      for (const seg of pathSegments) {
        const collapsed = seg.replace(/[-_.]+/g, "");
        if (SENSITIVE_PATH_TERMS.has(collapsed)) {
          return true;
        }
      }

      const params = new URLSearchParams(window.location.search);
      for (const key of params.keys()) {
        const lowerKey = key.toLowerCase();
        if (SENSITIVE_PATH_TERMS.has(lowerKey)) {
          return true;
        }
        const keyTokens = lowerKey.split(/[/_\-.]+/).filter(Boolean);
        if (keyTokens.some((token) => SENSITIVE_PATH_TERMS.has(token))) {
          return true;
        }
      }

      return false;
    } catch (error) {
      return true;
    }
  }

  function isDataEntryField(element) {
    if (element.tagName !== "INPUT") {
      return true;
    }
    return !NON_ENTRY_INPUT_TYPES.has((element.getAttribute("type") || "text").toLowerCase());
  }

  // A hidden sign-in pop-up (display:none or visibility:hidden) must not make
  // the page look private.
  function isVisible(element) {
    return element.getClientRects().length > 0 && window.getComputedStyle(element).visibility !== "hidden";
  }

  /** A7.4: only fields the reader can see count.
   *   1. a visible password box -> private
   *   2. PRIVATE_EMAIL_TEL_PER_FORM or more visible email/tel boxes in one <form> -> private
   *      (one newsletter box is not; a number box never counts here)
   *   3. PRIVATE_FORM_FIELD_COUNT or more visible data-entry fields -> private */
  function hasPrivateFormSurface() {
    if ([...document.querySelectorAll("input[type='password']")].some(isVisible)) {
      return true;
    }

    const contactBoxesPerForm = new Map();
    for (const input of document.querySelectorAll("input[type='email'], input[type='tel']")) {
      if (!input.form || !isVisible(input)) continue;
      const count = (contactBoxesPerForm.get(input.form) || 0) + 1;
      if (count >= PRIVATE_EMAIL_TEL_PER_FORM) return true;
      contactBoxesPerForm.set(input.form, count);
    }

    const editableCount = [
      ...document.querySelectorAll("input, textarea, select, [contenteditable=''], [contenteditable='true']")
    ].filter((element) => isDataEntryField(element) && isVisible(element)).length;
    return editableCount >= PRIVATE_FORM_FIELD_COUNT;
  }

  function visibleTextFrom(node) {
    const clone = node.cloneNode(true);
    clone
      .querySelectorAll("script, style, noscript, input, textarea, select, button, form, [contenteditable=''], [contenteditable='true']")
      .forEach((child) => child.remove());
    return (clone.innerText || clone.textContent || "").replace(/\s+/g, " ").trim();
  }

  // Visible h1-h3 texts of the chosen content node, in page order, as "## " lines.
  // Menus, sidebars, footers and forms are skipped; so are hidden headings.
  function headingLinesFrom(node) {
    const seen = new Set();
    const lines = [];
    for (const heading of node.querySelectorAll("h1, h2, h3")) {
      if (lines.length >= MAX_HEADINGS) break;
      if (heading.closest(NON_CONTENT_HEADING_PARENTS)) continue;
      if (heading.getClientRects().length === 0) continue;
      if (window.getComputedStyle(heading).visibility === "hidden") continue;
      const text = (heading.innerText || heading.textContent || "").replace(/\s+/g, " ").trim();
      if (!text || seen.has(text)) continue;
      seen.add(text);
      lines.push(HEADING_PREFIX + text.slice(0, MAX_HEADING_CHARS).trim());
    }
    return lines;
  }

  function extractReadableContent() {
    if (!rules || detectApplicationPage() || detectSensitivePage() || hasPrivateFormSurface()) {
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
        const headingBlock = headingLinesFrom(node).join("\n");
        if (!headingBlock) {
          return text.slice(0, MAX_CONTENT_CHARS);
        }
        // The cap covers headings + body together.
        return headingBlock + "\n" + text.slice(0, Math.max(MAX_CONTENT_CHARS - headingBlock.length - 1, 0));
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
    (event) => {
      // A click on a link only navigates away (A39.2); copy / expand / "show more"
      // clicks still count.
      if (event.target instanceof Element && event.target.closest("a[href]")) return;
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

  try {
    chrome.storage.local.get({ [RULES_KEY]: null }, (stored) => {
      if (!chrome.runtime.lastError) applyRules(stored[RULES_KEY]);
    });
    chrome.storage.onChanged.addListener((changes, area) => {
      if (area === "local" && changes[RULES_KEY]) applyRules(changes[RULES_KEY].newValue);
    });
  } catch (error) {
    // No storage access: rules stay null and no text is read.
  }

  computeScrollDepth();
  intervalId = setInterval(() => sendSignal("CHROME_PAGE_SIGNAL"), SIGNAL_INTERVAL_MS);
  // pagehide fires on every unload, including pages kept in the back/forward
  // cache, where beforeunload can be skipped.
  window.addEventListener("pagehide", () => sendSignal("CHROME_PAGE_UNLOAD"));
})();
