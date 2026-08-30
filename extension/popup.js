/**
 * popup.js - Echo Chrome Extension popup (architecture §12.3)
 *
 * Quick search   → POST /retrieval/query (full LangGraph RSE, same as dashboard)
 * Recent items   → GET /wellbeing/recent?limit=5
 * Pause tracking → chrome.storage.local.echo_tracking_paused
 *                  (background.js gates every backend call on this flag)
 * Dashboard link → the React app
 */

const BACKEND_URL = "http://localhost:8000";
const DASHBOARD_URL = "http://localhost:3000";
const PAUSE_KEY = "echo_tracking_paused";

const pauseToggle = document.getElementById("pauseToggle");
const pauseLabel = document.getElementById("pauseLabel");
const searchForm = document.getElementById("searchForm");
const searchInput = document.getElementById("searchInput");
const searchBtn = document.getElementById("searchBtn");
const answerBox = document.getElementById("answer");
const recentList = document.getElementById("recentList");
const statusEl = document.getElementById("status");
const dashboardLink = document.getElementById("dashboardLink");

dashboardLink.href = DASHBOARD_URL;

/* ── Pause / resume tracking ────────────────────────────────────────────── */

function renderPauseState(paused) {
  pauseToggle.setAttribute("aria-checked", String(paused));
  pauseLabel.textContent = paused ? "paused" : "tracking";
}

chrome.storage.local.get({ [PAUSE_KEY]: false }, (stored) => {
  renderPauseState(Boolean(stored[PAUSE_KEY]));
});

pauseToggle.addEventListener("click", () => {
  const paused = pauseToggle.getAttribute("aria-checked") !== "true";
  chrome.storage.local.set({ [PAUSE_KEY]: paused }, () => renderPauseState(paused));
});

/* ── Quick search ───────────────────────────────────────────────────────── */

searchForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const query = searchInput.value.trim();
  if (!query) {
    return;
  }

  searchBtn.disabled = true;
  answerBox.className = "answer visible thinking";
  answerBox.textContent = "searching your memory…";

  try {
    const response = await fetch(`${BACKEND_URL}/retrieval/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, session_id: null })
    });
    if (!response.ok) {
      throw new Error(`backend returned ${response.status}`);
    }
    const data = await response.json();
    answerBox.className = "answer visible";
    answerBox.textContent = data.final_answer || "No answer returned.";
  } catch (error) {
    answerBox.className = "answer visible thinking";
    answerBox.textContent = "Echo backend is offline — start it and try again.";
    console.warn("[Echo popup] query failed:", error);
  } finally {
    searchBtn.disabled = false;
  }
});

/* ── Recent items ───────────────────────────────────────────────────────── */

function renderRecent(items) {
  recentList.replaceChildren();

  if (!items.length) {
    const li = document.createElement("li");
    const span = document.createElement("span");
    span.className = "empty";
    span.textContent = "nothing captured yet";
    li.appendChild(span);
    recentList.appendChild(li);
    return;
  }

  for (const item of items) {
    const li = document.createElement("li");

    const dot = document.createElement("span");
    dot.className = `dot ${item.source_type || "chrome"}`;
    li.appendChild(dot);

    const title = document.createElement("span");
    title.className = "title";
    title.textContent = item.title || "(untitled)";
    title.title = item.title || "";
    li.appendChild(title);

    recentList.appendChild(li);
  }
}

async function loadRecent() {
  try {
    const response = await fetch(`${BACKEND_URL}/wellbeing/recent?limit=5`);
    if (!response.ok) {
      throw new Error(`backend returned ${response.status}`);
    }
    const data = await response.json();
    renderRecent(data.items || []);
    statusEl.textContent = "connected";
    statusEl.className = "status";
  } catch (error) {
    renderRecent([]);
    statusEl.textContent = "backend offline";
    statusEl.className = "status offline";
    console.warn("[Echo popup] recent fetch failed:", error);
  }
}

loadRecent();
