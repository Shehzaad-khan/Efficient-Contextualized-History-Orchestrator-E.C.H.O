/**
 * Echo popup: the pause switch is the only control.
 * background.js checks this local flag before every backend request.
 */
const PAUSE_KEY = "echo_tracking_paused";

const pauseToggle = document.getElementById("pauseToggle");
const pauseLabel = document.getElementById("pauseLabel");

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
