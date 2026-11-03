import { useEffect, useState } from 'react'
import { motion } from 'framer-motion'
import { ChevronDown, Flag, Hourglass, Percent } from 'lucide-react'
import Counter from '../components/Counter'
import SourceBadge from '../components/SourceBadge'
import StatCard from '../components/StatCard'
import Switch from '../components/Switch'
import { fetchRegretAnalytics, refreshRegretAnalytics, toggleRegret } from '../lib/api'
import { HOUR_LABELS, formatDuration, formatWhen } from '../lib/format'
import type { RegretAnalytics, RegrettedItem } from '../lib/types'

const EASE_OUT = [0.22, 1, 0.36, 1] as const

/** "11 PM" for hour 23, in the user's own 12- or 24-hour style. */
function hourName(hour: number): string {
  return new Date(2000, 0, 1, hour % 24).toLocaleTimeString(undefined, { hour: 'numeric' })
}

/** "11 PM–12 AM": the whole hour a bar stands for. */
function hourRange(hour: number): string {
  return `${hourName(hour)}–${hourName(hour + 1)}`
}

function momentsText(count: number): string {
  return `${count} ${count === 1 ? 'moment' : 'moments'}`
}

// The day in four parts, so the shape reads at a glance without counting 24 bars.
const DAY_PARTS = [
  { label: 'night', range: '12–6 AM', from: 0, to: 6 },
  { label: 'morning', range: '6 AM–12 PM', from: 6, to: 12 },
  { label: 'afternoon', range: '12–6 PM', from: 12, to: 18 },
  { label: 'evening', range: '6 PM–12 AM', from: 18, to: 24 },
] as const

const CHART_HEIGHT = 96

/**
 * Hour histogram: when in the day the moments you flagged fall. Each bar is one hour; hovering or
 * focusing one names the hour and its count in the line above, which otherwise names the busiest hour.
 */
function HourSparks({ byHour }: { byHour: RegretAnalytics['by_hour'] }) {
  const [picked, setPicked] = useState<number | null>(null)
  const counts = new Map(byHour.map((row) => [row.hour, row.regret_count]))
  const countAt = (hour: number) => counts.get(hour) ?? 0
  const hours = Array.from({ length: 24 }, (_, hour) => hour)
  const max = Math.max(...hours.map(countAt), 1)
  const total = hours.reduce((sum, hour) => sum + countAt(hour), 0)
  const peak = total > 0 ? hours.reduce((best, hour) => (countAt(hour) > countAt(best) ? hour : best), 0) : null

  let readout: string
  if (picked !== null) {
    const count = countAt(picked)
    readout = count > 0 ? `${hourRange(picked)} · ${momentsText(count)} flagged` : `${hourRange(picked)} · nothing flagged`
  } else if (peak !== null) {
    readout = `Most flagged: ${hourRange(peak)} · ${momentsText(countAt(peak))}`
  } else {
    readout = 'Nothing flagged yet.'
  }

  return (
    <div className="grid grid-cols-1 gap-8 lg:grid-cols-[1.8fr_1fr]">
      <div>
        <p className="mb-4 min-h-[1.25rem] font-mono text-xs text-ember-300" aria-live="polite">
          {readout}
        </p>

        <div role="group" aria-label="Flagged moments by hour of the day" className="flex items-end gap-[3px]" style={{ height: CHART_HEIGHT }}>
          {hours.map((hour) => {
            const count = countAt(hour)
            const active = picked === hour
            return (
              <button
                key={hour}
                type="button"
                onMouseEnter={() => setPicked(hour)}
                onMouseLeave={() => setPicked(null)}
                onFocus={() => setPicked(hour)}
                onBlur={() => setPicked(null)}
                aria-label={`${hourRange(hour)}: ${count === 0 ? 'nothing flagged' : `${momentsText(count)} flagged`}`}
                className="flex h-full flex-1 flex-col items-center justify-end gap-1 rounded-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ember-400/70"
              >
                {count > 0 && <span className="font-mono text-[10px] leading-none text-ember-300">{count}</span>}
                <motion.span
                  className={`block w-full rounded-t-sm transition-colors duration-150 ${
                    count > 0 ? (active ? 'bg-ember-400' : 'bg-ember-500/80') : active ? 'bg-white/[0.18]' : 'bg-white/[0.07]'
                  }`}
                  initial={{ height: 3 }}
                  whileInView={{ height: count > 0 ? Math.max(14, (count / max) * (CHART_HEIGHT - 22)) : 3 }}
                  viewport={{ once: true }}
                  transition={{ duration: 0.6, ease: EASE_OUT, delay: hour * 0.015 }}
                />
              </button>
            )
          })}
        </div>

        {/* One label every three hours, written the way the heatmap writes them (12a, 3a … 9p) */}
        <div className="mt-1.5 flex gap-[3px]" aria-hidden="true">
          {hours.map((hour) => (
            <span key={hour} className="flex flex-1 justify-center font-mono text-[10px] text-ink-mute">
              <span className="whitespace-nowrap">{HOUR_LABELS[hour]}</span>
            </span>
          ))}
        </div>
      </div>

      <dl className="grid grid-cols-2 gap-3" aria-label="Flagged moments by part of the day">
        {DAY_PARTS.map((part) => {
          const inPart = hours.filter((hour) => hour >= part.from && hour < part.to).reduce((sum, hour) => sum + countAt(hour), 0)
          return (
            <div key={part.label} className="rounded-card border border-white/[0.06] bg-night-900/50 px-4 py-3">
              <dt className="meta capitalize">{part.label}</dt>
              <dd className="mt-1 font-display text-3xl text-ink" style={{ fontVariationSettings: '"opsz" 32' }}>
                {inPart}
              </dd>
              <dd className="font-mono text-[10px] text-ink-faint">{part.range}</dd>
            </div>
          )
        })}
      </dl>
    </div>
  )
}

/** "<1m" under half a minute and "no time" at zero, so a flag never reads as "0m" when the time is merely short. */
function flagTime(seconds: number): string {
  if (seconds <= 0) return 'no time'
  return seconds < 30 ? '<1m' : formatDuration(seconds)
}

/**
 * The category list of the flagged moments: each row opens the items that belong to it, so a count
 * like "9 flags" can be traced to the pages, videos and mails behind it. Only items still flagged are
 * listed, so the list matches the count.
 */
function FlagCategories({
  categories,
  items,
  isMarked,
}: {
  categories: RegretAnalytics['by_category']
  items: RegrettedItem[]
  isMarked: (item: RegrettedItem) => boolean
}) {
  const [open, setOpen] = useState<ReadonlySet<string>>(new Set())

  if (categories.length === 0) return <p className="text-sm text-ink-mute">Nothing flagged yet.</p>
  const most = Math.max(...categories.map((row) => row.regret_count), 1)

  function toggle(groupName: string) {
    setOpen((current) => {
      const next = new Set(current)
      if (next.has(groupName)) next.delete(groupName)
      else next.add(groupName)
      return next
    })
  }

  return (
    <ul className="space-y-1" aria-label="Flags by category">
      {categories.map((row) => {
        const isOpen = open.has(row.group_name)
        const panelId = `flag-items-${row.group_name}`
        const inCategory = items.filter((item) => item.group_name === row.group_name && isMarked(item))
        return (
          <li key={row.group_name} className="border-b border-white/[0.05] last:border-0">
            <button
              type="button"
              onClick={() => toggle(row.group_name)}
              aria-expanded={isOpen}
              aria-controls={isOpen ? panelId : undefined}
              title={isOpen ? 'Hide the items' : 'Show the items'}
              className="group grid w-full grid-cols-[1fr_auto] items-center gap-x-4 gap-y-2 py-3.5 text-left md:grid-cols-[11rem_1fr_11rem]"
            >
              <span className="flex items-center gap-1.5 text-sm capitalize text-ink-soft transition group-hover:text-ink md:col-start-1 md:row-start-1">
                <ChevronDown
                  size={14}
                  aria-hidden="true"
                  className={`shrink-0 text-ember-400 transition-transform duration-200 ${isOpen ? 'rotate-0' : '-rotate-90'}`}
                />
                {row.group_name}
              </span>
              <span className="font-mono text-xs text-ink-mute md:col-start-3 md:row-start-1 md:text-right">
                {row.regret_count} {row.regret_count === 1 ? 'flag' : 'flags'} · {flagTime(row.total_seconds)}
              </span>
              <span className="col-span-2 block h-[3px] rounded-full bg-night-700/60 md:col-span-1 md:col-start-2 md:row-start-1" aria-hidden="true">
                <span
                  className="block h-full rounded-full bg-gradient-to-r from-ember-600 to-ember-400"
                  style={{ width: `${Math.max(4, (row.regret_count / most) * 100)}%` }}
                />
              </span>
            </button>

            {isOpen && (
              <motion.div
                id={panelId}
                initial={{ opacity: 0, y: -4 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.2, ease: EASE_OUT }}
                className="mb-3 rounded-card border border-white/[0.06] bg-night-900/60 px-4 py-1"
              >
                {inCategory.length === 0 ? (
                  <p className="py-3 text-sm text-ink-mute">No items to show for this category.</p>
                ) : (
                  <ul className="divide-y divide-white/[0.05]">
                    {inCategory.map((item) => (
                      <li key={item.memory_id} className="flex items-start gap-3 py-2.5">
                        <div className="w-20 shrink-0 pt-0.5">
                          <SourceBadge source={item.source_type} />
                        </div>
                        <div className="min-w-0 flex-1">
                          <p className="truncate text-sm text-ink-soft">{item.title?.trim() || 'Untitled'}</p>
                          {item.latest_note && (
                            <p className="mt-0.5 truncate font-display text-[13px] italic text-ember-300/90">
                              “{item.latest_note}”
                            </p>
                          )}
                        </div>
                        <span className="hidden shrink-0 font-mono text-[11px] text-ink-faint sm:block">
                          {formatWhen(item.last_marked_at ?? item.created_at)}
                        </span>
                      </li>
                    ))}
                  </ul>
                )}
                {inCategory.length > 0 && inCategory.length < row.regret_count && (
                  <p className="border-t border-white/[0.05] py-2 text-xs text-ink-faint">
                    Showing the latest {inCategory.length} of {row.regret_count}.
                  </p>
                )}
              </motion.div>
            )}
          </li>
        )
      })}
    </ul>
  )
}

/**
 * The "Reflections" view of Patterns: the moments the user flagged, in their own words. The page around
 * it (title, view switch) is `PatternsPage`.
 */
export default function ReflectionsView() {
  const [analytics, setAnalytics] = useState<RegretAnalytics | null>(null)
  // Items whose mark was switched off stay in the list (so a slip can be switched back on); this holds
  // each item's real state as the server last reported it. No entry yet = still marked, as listed.
  const [marks, setMarks] = useState<Record<string, boolean>>({})
  const [busyId, setBusyId] = useState<string | null>(null)
  const [failedId, setFailedId] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    void fetchRegretAnalytics().then((data) => !cancelled && setAnalytics(data))
    return () => {
      cancelled = true
    }
  }, [])

  const isMarked = (item: RegrettedItem) => marks[item.memory_id] ?? true

  /** The same route marks and un-marks; the reply says which one happened. */
  async function flipMark(item: RegrettedItem) {
    const id = item.memory_id
    setBusyId(id)
    setFailedId(null)
    try {
      const result = await toggleRegret(id, undefined, isMarked(item))
      setMarks((current) => ({ ...current, [id]: result.regretted }))
      // The rate, hours and groups change with the mark; the item rows stay as listed.
      refreshRegretAnalytics()
        .then((fresh) => setAnalytics((current) => (current ? { ...fresh, items: current.items } : current)))
        .catch(() => undefined) // keep the figures already shown
    } catch {
      setFailedId(id) // the mark stays as it was
    } finally {
      setBusyId(null)
    }
  }

  return (
    <div>
      <motion.p
        className="max-w-[56ch] text-[15px] leading-7 text-ink-mute"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.6, delay: 0.35 }}
      >
        These are the moments you chose to flag — Echo never marks anything itself, never scores you, and never
        hides what you flagged. It only holds up the mirror you asked for.
      </motion.p>

      {analytics === null ? (
        <div className="mt-8 grid grid-cols-1 gap-6 lg:grid-cols-3" role="status" aria-label="Loading reflections">
          {[0, 1, 2].map((index) => (
            <div key={index} className="card h-32 animate-pulse bg-night-850/60" />
          ))}
        </div>
      ) : (
        <>
          {/* Stat band: the first two are TODAY's, the third counts every flag still on; the labels say so */}
          <div className="mt-8 grid grid-cols-1 gap-3 sm:grid-cols-3">
            <StatCard
              icon={Percent}
              label="flagged share · today"
              value={<Counter value={analytics.rate.regret_rate_percent} format={(v) => `${v.toFixed(1)}%`} />}
              note={
                <>
                  {analytics.rate.total_seconds > 0
                    ? `${formatDuration(analytics.rate.regretted_seconds)} of ${formatDuration(analytics.rate.total_seconds)} today`
                    : 'No time recorded yet today'}
                  <span className="mt-2 block h-[3px] rounded-full bg-night-700/60" aria-hidden="true">
                    <span
                      className="block h-full rounded-full bg-gradient-to-r from-ember-600 to-ember-400 transition-[width] duration-500"
                      style={{ width: `${Math.min(100, analytics.rate.regret_rate_percent)}%` }}
                    />
                  </span>
                </>
              }
            />
            <StatCard
              icon={Hourglass}
              label="time on flagged items · today"
              value={<Counter value={analytics.rate.regretted_seconds} format={formatDuration} />}
              note="What you spent today on items you still have flagged."
            />
            <StatCard
              icon={Flag}
              label="items flagged · all time"
              value={<Counter value={analytics.items.filter(isMarked).length} />}
              note="Switch any of them off in the list below."
            />
          </div>

          <section className="card mt-8 p-6" aria-labelledby="hours-heading">
            <h2 id="hours-heading" className="meta mb-2">
              when you flag moments
            </h2>
            <p className="mb-6 max-w-[70ch] text-xs leading-5 text-ink-mute">
              Each bar is one hour of the day, in your local time. A taller bar means more of your flagged moments fall
              in that hour. It counts every flag you still have on, not just today’s.
            </p>
            <HourSparks byHour={analytics.by_hour} />
          </section>

          <section className="card mt-6 p-6" aria-labelledby="category-heading">
            <h2 id="category-heading" className="meta mb-3">
              by category · all your flags
            </h2>
            <FlagCategories categories={analytics.by_category} items={analytics.items} isMarked={isMarked} />
          </section>

          {/* The marked items: the user's own notes given typographic weight, two to a row */}
          <section className="mt-10" aria-labelledby="marked-heading">
            <h2 id="marked-heading" className="meta mb-5">
              what you flagged
            </h2>
            <ul className="grid grid-cols-1 gap-4 md:grid-cols-2">
              {analytics.items.map((item, index) => (
                <motion.li
                  key={item.memory_id}
                  className={`card card-hover px-5 py-4 transition-opacity duration-300 ${isMarked(item) ? '' : 'opacity-60'}`}
                  initial={{ opacity: 0, y: 12 }}
                  whileInView={{ opacity: isMarked(item) ? 1 : 0.6, y: 0 }}
                  viewport={{ once: true, margin: '-30px' }}
                  transition={{ duration: 0.45, ease: EASE_OUT, delay: (index % 4) * 0.05 }}
                >
                  <div className="flex flex-wrap items-center gap-3">
                    <SourceBadge source={item.source_type} />
                    <span className="meta text-ink-faint">{formatWhen(item.last_marked_at ?? item.created_at)}</span>
                    <div className="ml-auto flex items-center gap-3">
                      <span className="meta whitespace-nowrap text-ink-faint" aria-hidden="true">
                        {isMarked(item) ? 'marked' : 'unmarked'}
                      </span>
                      <Switch
                        checked={isMarked(item)}
                        onChange={() => void flipMark(item)}
                        disabled={busyId === item.memory_id}
                        label={`Flag on: ${item.title || 'untitled'}`}
                      />
                    </div>
                  </div>
                  {failedId === item.memory_id && (
                    <p role="alert" className="mt-2 text-xs leading-5 text-ink-soft">
                      Couldn’t update the mark just now — it’s as it was. Try again in a moment.
                    </p>
                  )}
                  <p className="mt-2 text-[15px] font-medium text-ink">{item.title || '(untitled)'}</p>
                  {item.latest_note && (
                    <p
                      className="mt-2 border-l-2 border-ember-500/40 pl-3 font-display text-[15px] italic leading-relaxed text-ember-300/90"
                      style={{ fontVariationSettings: '"opsz" 20' }}
                    >
                      “{item.latest_note}”
                    </p>
                  )}
                </motion.li>
              ))}
              {analytics.items.length === 0 && (
                <li className="font-display text-lg italic text-ink-mute md:col-span-2">Nothing flagged. The mirror is clear tonight.</li>
              )}
            </ul>
          </section>
        </>
      )}
    </div>
  )
}
