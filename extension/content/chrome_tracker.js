if (!chrome.extension.inIncognitoContext) {
  let maxScrollDepth = 0;
  let interactionCount = 0;
  let intervalId = null;
  const sensitivePathTerms = new Set([
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
    const appDomains = [
      "slack.com",
      "app.slack.com",
      "jira.com",
      "atlassian.net",
      "notion.so",
      "figma.com",
      "linear.app",
      "trello.com",
      "asana.com",
      "confluence.atlassian.net",
      "mail.google.com"
    ];
    return appDomains.some((domain) => host === domain || host.endsWith(`.${domain}`));
  }

  function detectSensitivePage() {
    const pathParts = window.location.pathname
      .toLowerCase()
      .split("/")
      .filter(Boolean);
    const target = `${window.location.pathname}?${window.location.search}`.toLowerCase();
    return (
      pathParts.some((part) => sensitivePathTerms.has(part)) ||
      [...sensitivePathTerms].some((term) => target.includes(`/${term}`) || target.includes(`${term}=`))
    );
  }

  function hasPrivateFormSurface() {
    if (document.querySelector("input[type='password'], input[type='email'], input[type='tel'], input[type='number']")) {
      return true;
    }
    const editableCount = document.querySelectorAll("input, textarea, select, [contenteditable=''], [contenteditable='true']").length;
    return editableCount >= 5;
  }

  function visibleTextFrom(node) {
    const clone = node.cloneNode(true);
    clone.querySelectorAll("script, style, noscript, input, textarea, select, button, form, [contenteditable=''], [contenteditable='true']").forEach((child) => child.remove());
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
      if (text.length >= 200) {
        return text.slice(0, 12000);
      }
    }

    return "";
  }

  function sendSignal(type) {
    try {
      if (!chrome.runtime?.id) {
        return;
      }

      const contentExtract = extractReadableContent();
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
          isAppPage: detectApplicationPage(),
          timestamp: Date.now()
        },
        () => {
          const lastError = chrome.runtime.lastError;
          if (lastError && lastError.message?.includes("context invalidated") && intervalId !== null) {
            clearInterval(intervalId);
            intervalId = null;
          }
        }
      );
    } catch (error) {
      if (intervalId !== null) {
        clearInterval(intervalId);
        intervalId = null;
      }
    }
  }

  window.addEventListener(
    "scroll",
    () => {
      computeScrollDepth();
    },
    { passive: true }
  );

  document.body?.addEventListener(
    "click",
    () => {
      interactionCount += 1;
    },
    { passive: true }
  );

  document.addEventListener("selectionchange", () => {
    const selectedText = document.getSelection()?.toString().trim();
    if (selectedText) {
      interactionCount += 1;
    }
  });

  computeScrollDepth();
  intervalId = setInterval(() => sendSignal("CHROME_PAGE_SIGNAL"), 5000);
  window.addEventListener("beforeunload", () => sendSignal("CHROME_PAGE_UNLOAD"));
}
