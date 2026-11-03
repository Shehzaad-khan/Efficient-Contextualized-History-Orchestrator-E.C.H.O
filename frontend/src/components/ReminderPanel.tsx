import Switch from './Switch'
import type { ReminderStatus } from '../lib/types'

// Shown until the backend's own numbers arrive (wba/regret_system.py, wba/pattern_detector.py).
const FALLBACK = { min_session_minutes: 15, min_flags: 3, lookback_days: 30, daily_limit: 2, cooldown_hours: 2 }

function Condition({ number, title, children }: { number: number; title: string; children: string }) {
  return (
    <li className="flex gap-4 rounded-card border border-white/[0.06] bg-night-900/50 p-5">
      <span className="grid h-8 w-8 shrink-0 place-items-center rounded-full border border-ember-500/30 font-mono text-sm text-ember-400">
        {number}
      </span>
      <span>
        <span className="block text-base font-medium text-ink">{title}</span>
        <span className="mt-1 block text-[15px] leading-6 text-ink-mute">{children}</span>
      </span>
    </li>
  )
}

/**
 * The Settings page's one place for reminders: the switch, and in a few words when a reminder
 * appears. The reminder card itself is `RegretReminderNotice`; the switch is the backend flag.
 */
export default function ReminderPanel({
  status,
  enabled,
  onChange,
}: {
  /** null until it loads; only its numbers and `available` are used */
  status: ReminderStatus | null
  enabled: boolean
  onChange: (next: boolean) => void
}) {
  const numbers = status ?? FALLBACK
  const paused = status !== null && !status.available
  const on = enabled && !paused

  return (
    <div className="card p-6 sm:p-8">
      <div className="flex items-center justify-between gap-6">
        <div>
          <p className="text-lg font-medium text-ink">Show me reminders</p>
          <p className="mt-0.5 text-[15px] leading-6 text-ink-mute">
            {on ? 'On. A small note can appear while you use Echo.' : 'Off. Echo will not show you any reminders.'}
          </p>
        </div>
        <Switch checked={enabled} onChange={onChange} label="Show me reminders" />
      </div>

      {paused && (
        <p role="status" className="mt-5 rounded-card border border-white/[0.08] bg-night-900/50 px-5 py-4 text-[15px] leading-6 text-ink-mute">
          Reminders are paused because Echo can’t reach the place that counts them. They return by themselves once it is back.
        </p>
      )}

      <div className={`transition-opacity duration-300 ${on ? '' : 'opacity-50'}`}>
        <h3 className="mb-4 mt-8 text-[15px] text-ink-soft">A reminder shows up only when all three are true:</h3>
        <ol className="grid grid-cols-1 gap-4 md:grid-cols-3">
          <Condition number={1} title="A long stretch">
            {`You have been on it for ${numbers.min_session_minutes}+ minutes.`}
          </Condition>
          <Condition number={2} title="A habit">
            {`You flagged this kind of thing ${numbers.min_flags}+ times around this hour in the last ${numbers.lookback_days} days.`}
          </Condition>
          <Condition number={3} title="Your own note">
            One of those flags has a note you wrote. It is the only thing shown.
          </Condition>
        </ol>
        <p className="mt-5 text-[15px] leading-6 text-ink-faint">
          At most {numbers.daily_limit} a day, {numbers.cooldown_hours} hours apart, and only while the Echo page is open.
        </p>
      </div>
    </div>
  )
}
