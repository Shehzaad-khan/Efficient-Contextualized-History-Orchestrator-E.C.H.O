import { ChevronDown } from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import type { ReactNode } from 'react'

/**
 * One number of a stat row: an icon, a small label, the number, and an optional line under it. A card
 * with an `action` is a button that opens something (the Time view's "sessions" card opens the day's
 * sessions); without one it is plain.
 */
export default function StatCard({
  icon: Icon,
  label,
  value,
  note,
  action,
}: {
  icon: LucideIcon
  label: string
  value: ReactNode
  /** A short line under the number saying what it is made of ("2m of 2m today"). */
  note?: ReactNode
  /** Present when the card can be opened: what it opens, and whether it is open now. */
  action?: { hint: string; hideHint: string; open: boolean; controls: string; onToggle: () => void }
}) {
  const body = (
    <>
      <span aria-hidden="true" className="pointer-events-none absolute -right-8 -top-10 h-28 w-28 rounded-full bg-ember-500/10 blur-2xl" />
      <span className="relative flex items-center gap-2.5">
        <span className="grid h-7 w-7 shrink-0 place-items-center rounded-full border border-ember-500/25 bg-ember-500/10 text-ember-400">
          <Icon size={14} strokeWidth={1.75} aria-hidden="true" />
        </span>
        <span className="meta leading-tight">{label}</span>
      </span>
      <span className="relative mt-4 block whitespace-nowrap font-display text-3xl text-ink sm:text-4xl" style={{ fontVariationSettings: '"opsz" 40' }}>
        {value}
      </span>
      {note && <span className="relative mt-2 block text-xs leading-5 text-ink-mute">{note}</span>}
      {action && (
        <span className="relative mt-4 flex items-center gap-1 font-mono text-[10px] uppercase tracking-wider text-ember-400 transition group-hover:text-ember-300">
          {action.open ? action.hideHint : action.hint}
          <ChevronDown
            size={12}
            aria-hidden="true"
            className={`transition-transform duration-200 ${action.open ? 'rotate-180' : '-rotate-90'}`}
          />
        </span>
      )}
    </>
  )
  const base =
    'relative flex flex-col overflow-hidden rounded-card border bg-gradient-to-b from-night-800/80 to-night-850 p-5 text-left shadow-raise transition duration-200'

  if (!action) return <div className={`${base} border-white/[0.07]`}>{body}</div>
  return (
    <button
      type="button"
      onClick={action.onToggle}
      aria-expanded={action.open}
      aria-controls={action.open ? action.controls : undefined}
      className={`${base} group ${
        action.open
          ? 'border-ember-500/55 bg-ember-500/[0.06]'
          : 'border-white/[0.07] hover:-translate-y-0.5 hover:border-ember-500/40'
      } focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember-400/60`}
    >
      {body}
    </button>
  )
}
