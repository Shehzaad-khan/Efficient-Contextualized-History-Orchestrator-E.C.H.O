import { useEffect, useState } from 'react'
import { motion } from 'framer-motion'
import PageTitle from '../components/PageTitle'
import Counter from '../components/Counter'
import SourceBadge from '../components/SourceBadge'
import { fetchRegretAnalytics } from '../lib/api'
import { formatDuration, formatWhen } from '../lib/format'
import type { RegretAnalytics } from '../lib/types'

const EASE_OUT = [0.22, 1, 0.36, 1] as const

/** Hour histogram — when regret marks tend to happen. */
function HourSparks({ byHour }: { byHour: RegretAnalytics['by_hour'] }) {
  const counts = new Map(byHour.map((row) => [row.hour, row.regret_count]))
  const max = Math.max(...byHour.map((row) => row.regret_count), 1)
  return (
    <div role="img" aria-label="Regret marks by hour of day" className="flex h-20 items-end gap-[3px]">
      {Array.from({ length: 24 }, (_, hour) => {
        const count = counts.get(hour) ?? 0
        return (
          <div key={hour} className="flex flex-1 flex-col items-center gap-1">
            <motion.span
              className={`w-full rounded-t-sm ${count > 0 ? 'bg-ember-500/80' : 'bg-white/[0.05]'}`}
              initial={{ height: 2 }}
              whileInView={{ height: count > 0 ? `${Math.max(12, (count / max) * 64)}px` : '2px' }}
              viewport={{ once: true }}
              transition={{ duration: 0.6, ease: EASE_OUT, delay: hour * 0.015 }}
              title={count > 0 ? `${count} mark(s) around ${hour}:00` : undefined}
            />
            <span className="font-mono text-[8px] text-ink-faint">{hour % 6 === 0 ? hour : ''}</span>
          </div>
        )
      })}
    </div>
  )
}

export default function ReflectionsPage() {
  const [analytics, setAnalytics] = useState<RegretAnalytics | null>(null)

  useEffect(() => {
    let cancelled = false
    void fetchRegretAnalytics().then((data) => !cancelled && setAnalytics(data))
    return () => {
      cancelled = true
    }
  }, [])

  return (
    <div>
      <PageTitle
        eyebrow="reflections"
        title="In your own words."
        aside={<p className="meta">only you decide what belongs here</p>}
      />

      <motion.p
        className="mt-6 max-w-[52ch] text-[15px] leading-7 text-ink-mute"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.6, delay: 0.35 }}
      >
        These are the moments you chose to flag — Echo never marks anything itself, never scores you, and never
        hides what you flagged. It only holds up the mirror you asked for.
      </motion.p>

      {analytics === null ? (
        <div className="mt-10 grid grid-cols-1 gap-6 lg:grid-cols-3" role="status" aria-label="Loading reflections">
          {[0, 1, 2].map((index) => (
            <div key={index} className="card h-32 animate-pulse bg-night-850/60" />
          ))}
        </div>
      ) : (
        <>
          {/* Stat band */}
          <div className="mt-10 grid grid-cols-1 gap-px overflow-hidden rounded-card border border-white/[0.06] bg-white/[0.04] sm:grid-cols-3">
            <div className="bg-night-850 px-6 py-6">
              <p className="meta">of today's time, flagged</p>
              <p className="mt-2 font-display text-4xl text-ink" style={{ fontVariationSettings: '"opsz" 48' }}>
                <Counter value={analytics.rate.regret_rate_percent} format={(v) => `${v.toFixed(1)}%`} />
              </p>
            </div>
            <div className="bg-night-850 px-6 py-6">
              <p className="meta">flagged time</p>
              <p className="mt-2 font-display text-4xl text-ink" style={{ fontVariationSettings: '"opsz" 48' }}>
                <Counter value={analytics.rate.regretted_seconds} format={formatDuration} />
              </p>
            </div>
            <div className="bg-night-850 px-6 py-6">
              <p className="meta">items currently marked</p>
              <p className="mt-2 font-display text-4xl text-ink" style={{ fontVariationSettings: '"opsz" 48' }}>
                <Counter value={analytics.items.length} />
              </p>
            </div>
          </div>

          <div className="mt-8 grid grid-cols-1 gap-8 lg:grid-cols-[1fr_1.3fr]">
            {/* When + where */}
            <div className="space-y-8">
              <section className="card p-6" aria-labelledby="hours-heading">
                <h2 id="hours-heading" className="meta mb-6">
                  when the marks happen
                </h2>
                <HourSparks byHour={analytics.by_hour} />
              </section>

              <section className="card p-6" aria-labelledby="category-heading">
                <h2 id="category-heading" className="meta mb-5">
                  by category
                </h2>
                <ul className="space-y-3">
                  {analytics.by_category.map((row) => (
                    <li key={row.group_name} className="flex items-baseline justify-between gap-4 border-b border-white/[0.05] pb-2.5">
                      <span className="text-sm capitalize text-ink-soft">{row.group_name}</span>
                      <span className="font-mono text-xs text-ink-mute">
                        {row.regret_count} marks · {formatDuration(row.total_seconds)}
                      </span>
                    </li>
                  ))}
                  {analytics.by_category.length === 0 && <li className="text-sm text-ink-mute">No marks yet.</li>}
                </ul>
              </section>
            </div>

            {/* The marked items — the user's own notes given typographic weight */}
            <section aria-labelledby="marked-heading">
              <h2 id="marked-heading" className="meta mb-5">
                what you flagged
              </h2>
              <ul className="space-y-4">
                {analytics.items.map((item, index) => (
                  <motion.li
                    key={item.memory_id}
                    className="card card-hover px-5 py-4"
                    initial={{ opacity: 0, y: 12 }}
                    whileInView={{ opacity: 1, y: 0 }}
                    viewport={{ once: true, margin: '-30px' }}
                    transition={{ duration: 0.45, ease: EASE_OUT, delay: (index % 5) * 0.05 }}
                  >
                    <div className="flex flex-wrap items-center gap-3">
                      <SourceBadge source={item.source_type} />
                      <span className="meta text-ink-faint">{formatWhen(item.last_marked_at ?? item.created_at)}</span>
                    </div>
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
                  <li className="font-display text-lg italic text-ink-mute">Nothing flagged. The mirror is clear tonight.</li>
                )}
              </ul>
            </section>
          </div>
        </>
      )}
    </div>
  )
}
