// The Settings page keeps its switches under this key; keep "Regret reminders" there in step.
const SETTINGS_STORAGE_KEY = 'echo-settings'

/** Remember the reminders switch where the Settings page reads it. The backend flag is what counts. */
export function rememberRemindersSetting(enabled: boolean): void {
  try {
    const stored = window.localStorage.getItem(SETTINGS_STORAGE_KEY)
    const current = stored ? (JSON.parse(stored) as Record<string, unknown>) : {}
    window.localStorage.setItem(SETTINGS_STORAGE_KEY, JSON.stringify({ ...current, regretReminders: enabled }))
  } catch {
    // Storage blocked: only the Settings switch would lag behind.
  }
}
