import { useEffect, useRef, useState } from 'react'
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion'
import {
  fetchRecent,
  fetchRegretReminder,
  fetchTodaySessions,
  isDemoMode,
  setReminderSettings,
  snoozeRegretRemindersToday,
} from '../lib/api'
import type { RegretReminder } from '../lib/types'

const EASE_OUT = [0.22, 1, 0.36, 1] as const

const CHECK_EVERY_MS = 2 * 60 * 1000
// Same numbers the backend uses (wba/regret_system.py MIN_SESSION_MINUTES_FOR_REMINDER,
// wba/wellbeing_analytics.py SESSION_GAP_MINUTES). Checking them here only saves a call;
// the backend decides on its own.
const MIN_SESSION_MINUTES = 15
const SESSION_GAP_MINUTES = 20

const SOURCE_LABEL = { gmail: 'Gmail', chrome: 'the web', youtube: 'YouTube' } as const

// The Settings page keeps its switches under this key; keep "Regret reminders" there in step.
const SETTINGS_STORAGE_KEY = 'echo-settings'

/** The backend sends naive UTC timestamps; read them as UTC rather than local time. */
function utcMs(value: string): number {
  const hasZone = /[zZ]|[+-]\d\d:?\d\d$/.test(value)
  return new Date(hasZone ? value : `${value}Z`).getTime()
}

function rememberRemindersOff(): void {
  try {
    const stored = window.localStorage.getItem(SETTINGS_STORAGE_KEY)
    const current = stored ? (JSON.parse(stored) as Record<string, unknown>) : {}
    window.localStorage.setItem(SETTINGS_STORAGE_KEY, JSON.stringify({ ...current, regretReminders: false }))
  } catch {
    // Storage blocked: the backend flag is what counts, only the Settings switch would lag.
  }
}

/**
 * Regret reminders (D3, architecture §11.6): a quiet, non-blocking card that shows the user's OWN
 * earlier note when a long session matches a habit they flagged.
 *
 * While the dashboard is open it checks every couple of minutes whether today's latest stored
 * session is still going (its last activity is within the session gap) and long enough, then asks
 * the backend, which applies every other rule (habit, cooldown, daily limits, disabled/snoozed).
 * It never writes, blocks or steals focus, and shows nothing in demo mode.
 */
export default function RegretReminderNotice() {
  const [reminder, setReminder] = useState<{ data: RegretReminder; minutes: number } | null>(null)
  const reduceMotion = useReducedMotion()
  // The backend counts a returned reminder against today's limit, so never ask while one is on screen.
  const showing = useRef(false)
  const checking = useRef(false)

  useEffect(() => {
    showing.current = reminder !== null
  }, [reminder])

  useEffect(() => {
    let cancelled = false

    async function check() {
      if (cancelled || checking.current || showing.current || isDemoMode()) return
      checking.current = true
      try {
        const sessions = await fetchTodaySessions()
        const latest = sessions[sessions.length - 1]
        if (!latest) return
        const idleMinutes = (Date.now() - utcMs(latest.session_end)) / 60000
        const minutes = latest.total_duration_seconds / 60
        if (idleMinutes > SESSION_GAP_MINUTES || minutes < MIN_SESSION_MINUTES) return

        const [newest] = await fetchRecent(1)
        if (!newest || isDemoMode()) return
        const found = await fetchRegretReminder(newest.source_type, minutes)
        if (found && !cancelled) setReminder({ data: found, minutes: Math.round(minutes) })
      } finally {
        checking.current = false
      }
    }

    const first = window.setTimeout(() => void check(), 10_000)
    const timer = window.setInterval(() => void check(), CHECK_EVERY_MS)
    return () => {
      cancelled = true
      window.clearTimeout(first)
      window.clearInterval(timer)
    }
  }, [])

  const dismiss = () => setReminder(null)

  function snooze() {
    dismiss()
    void snoozeRegretRemindersToday()
  }

  function disable() {
    dismiss()
    rememberRemindersOff()
    void setReminderSettings(false)
  }

  return (
    <div className="pointer-events-none fixed inset-x-4 bottom-24 z-overlay flex justify-end lg:inset-x-auto lg:bottom-6 lg:right-6">
      <AnimatePresence>
        {reminder && (
          <motion.aside
            role="status"
            aria-live="polite"
            aria-label="A note you left yourself"
            className="card pointer-events-auto w-full max-w-sm border-ember-500/20 p-5 shadow-lamp"
            initial={reduceMotion ? { opacity: 0 } : { opacity: 0, y: 16 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, transition: { duration: 0.2 } }}
            transition={{ duration: 0.4, ease: EASE_OUT }}
          >
            <p className="meta">
              about {reminder.minutes} min on {SOURCE_LABEL[reminder.data.source_type]}
            </p>
            <p className="mt-3 text-xs text-ink-mute">A note you left yourself:</p>
            <p
              className="mt-1.5 border-l-2 border-ember-500/40 pl-3 font-display text-[15px] italic leading-relaxed text-ember-300/90"
              style={{ fontVariationSettings: '"opsz" 20' }}
            >
              “{reminder.data.your_note}”
            </p>
            <div className="mt-4 flex flex-wrap items-center gap-2">
              <button type="button" onClick={dismiss} className="btn-ghost px-3.5 py-1.5 text-xs">
                Got it
              </button>
              <button type="button" onClick={snooze} className="btn-ghost px-3.5 py-1.5 text-xs">
                Snooze for today
              </button>
              <button
                type="button"
                onClick={disable}
                className="px-2 py-1.5 text-xs text-ink-mute underline-offset-4 transition hover:text-ink-soft hover:underline"
              >
                Turn off reminders
              </button>
            </div>
          </motion.aside>
        )}
      </AnimatePresence>
    </div>
  )
}
