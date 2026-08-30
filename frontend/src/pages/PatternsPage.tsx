import { useEffect, useMemo, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { Check, Plus, X } from 'lucide-react'
import PageTitle from '../components/PageTitle'
import Counter from '../components/Counter'
import SourceBadge from '../components/SourceBadge'
import {
  createGroup,
  decideSuggestion,
  fetchDaily,
  fetchGroups,
  fetchHeatmap,
  fetchReviewQueue,
  fetchWeeklyInsight,
} from '../lib/api'
import { DOW_LABELS, formatDuration } from '../lib/format'
import type { DailySummary, GroupSuggestion, HeatmapCell, UserGroup, WeeklyInsight } from '../lib/types'

const EASE_OUT = [0.22, 1, 0.36, 1] as const

const GROUP_ORDER = ['study', 'work', 'entertainment', 'personal', 'misc']

/* ── Category bars — thin, editorial, no chart library ─────────────────── */
function CategoryBars({ summary }: { summary: DailySummary }) {
  const max = Math.max(...summary.by_system_group.map((group) => group.total_seconds), 1)
  const rows = [...summary.by_system_group].sort(
    (a, b) => GROUP_ORDER.indexOf(a.group_name) - GROUP_ORDER.indexOf(b.group_name),
  )
  return (
    <div className="space-y-3.5" role="img" aria-label="Time by category today">
      {rows.map((group, index) => {
        const percent = summary.total_seconds ? Math.round((group.total_seconds / summary.total_seconds) * 100) : 0
        return (
          <div key={group.group_name}>
            <div className="flex items-baseline justify-between gap-4">
              <span className="text-sm capitalize text-ink-soft">{group.group_name}</span>
              <span className="font-mono text-xs text-ink-mute">
                {formatDuration(group.total_seconds)} · {percent}%
              </span>
            </div>
            <div className="mt-1.5 h-[3px] rounded-full bg-night-700/60">
              <motion.div
                className="h-full rounded-full bg-gradient-to-r from-ember-600 to-ember-400"
                initial={{ width: 0 }}
                whileInView={{ width: `${(group.total_seconds / max) * 100}%` }}
                viewport={{ once: true }}
                transition={{ duration: 0.9, ease: EASE_OUT, delay: 0.15 + index * 0.07 }}
              />
            </div>
          </div>
        )
      })}
    </div>
  )
}

/* ── Hour × day heatmap — lamplight intensity, regret ticks ─────────────── */
function Heatmap({ cells }: { cells: HeatmapCell[] }) {
  const byKey = useMemo(() => {
    const map = new Map<string, HeatmapCell>()
    for (const cell of cells) map.set(`${cell.day_of_week}-${cell.hour}`, cell)
    return map
  }, [cells])
  const max = Math.max(...cells.map((cell) => cell.total_seconds), 1)

  return (
    <div role="img" aria-label="Activity heatmap by hour and weekday; brighter cells mean more time; flagged cells carry a regret mark">
      <div className="overflow-x-auto pb-2">
        <div className="min-w-[560px]">
          {Array.from({ length: 7 }, (_, day) => (
            <div key={day} className="flex items-center gap-1">
              <span className="w-8 shrink-0 font-mono text-[9px] uppercase text-ink-faint">{DOW_LABELS[day]}</span>
              {Array.from({ length: 24 }, (_, hour) => {
                const cell = byKey.get(`${day}-${hour}`)
                const intensity = cell ? cell.total_seconds / max : 0
                return (
                  <motion.span
                    key={hour}
                    initial={{ opacity: 0 }}
                    whileInView={{ opacity: 1 }}
                    viewport={{ once: true }}
                    transition={{ duration: 0.3, delay: (day * 24 + hour) * 0.0015 }}
                    title={cell ? `${DOW_LABELS[day]} ${hour}:00 — ${formatDuration(cell.total_seconds)}${cell.has_regret ? ' · regret marked' : ''}` : undefined}
                    className="relative m-[1.5px] h-4 flex-1 rounded-[3px]"
                    style={{
                      backgroundColor:
                        intensity === 0 ? 'rgba(255,255,255,0.03)' : `rgba(245, 169, 71, ${0.08 + intensity * 0.78})`,
                    }}
                  >
                    {cell?.has_regret && (
                      <span aria-hidden="true" className="absolute right-0 top-0 h-1 w-1 rounded-full bg-youtube" />
                    )}
                  </motion.span>
                )
              })}
            </div>
          ))}
          <div className="mt-1 flex gap-1 pl-8">
            {Array.from({ length: 24 }, (_, hour) => (
              <span key={hour} className="m-[1.5px] flex-1 text-center font-mono text-[8px] text-ink-faint">
                {hour % 6 === 0 ? `${hour === 0 ? 12 : hour > 12 ? hour - 12 : hour}${hour < 12 ? 'a' : 'p'}` : ''}
              </span>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}

/* ── User groups ────────────────────────────────────────────────────────── */
function Groups({ groups, onCreate }: { groups: UserGroup[]; onCreate: (name: string) => Promise<void> }) {
  const [name, setName] = useState('')
  const [saving, setSaving] = useState(false)

  async function submit() {
    const trimmed = name.trim()
    if (!trimmed || saving) return
    setSaving(true)
    try {
      await onCreate(trimmed)
      setName('')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div>
      <ul className="space-y-3">
        {groups.map((group) => (
          <li key={group.group_id} className="flex items-center justify-between gap-4 border-b border-white/[0.05] pb-3">
            <div className="min-w-0">
              <p className="truncate text-sm font-medium text-ink">{group.group_name}</p>
              {group.description && <p className="mt-0.5 truncate text-xs text-ink-mute">{group.description}</p>}
            </div>
            <div className="shrink-0 text-right">
              <p className="font-mono text-xs text-ink-soft">{group.member_count} items</p>
              <p className="meta mt-0.5 text-[9px]">
                {group.auto_assignment_active ? 'auto-sorting on' : `${Math.max(0, 6 - group.member_count)} more to auto-sort`}
              </p>
            </div>
          </li>
        ))}
        {groups.length === 0 && <li className="text-sm text-ink-mute">No goal groups yet — name one below.</li>}
      </ul>

      <form
        className="mt-4 flex gap-2"
        onSubmit={(event) => {
          event.preventDefault()
          void submit()
        }}
      >
        <input
          value={name}
          onChange={(event) => setName(event.target.value)}
          placeholder="a goal, e.g. “Placement Prep”…"
          aria-label="New group name"
          className="input-study flex-1 px-3 py-2 text-sm"
        />
        <button type="submit" disabled={saving || !name.trim()} className="btn-ghost inline-flex items-center gap-1.5 disabled:opacity-40" aria-label="Create group">
          <Plus size={14} aria-hidden="true" /> add
        </button>
      </form>
    </div>
  )
}

/* ── Weekly review queue — human-in-the-loop for auto-assignments ────────── */
function ReviewQueue({
  suggestions,
  onDecide,
}: {
  suggestions: GroupSuggestion[]
  onDecide: (suggestionId: string, accept: boolean) => void
}) {
  if (suggestions.length === 0) {
    return <p className="text-sm text-ink-mute">Nothing waiting for review — the queue is clear.</p>
  }
  return (
    <ul className="space-y-3">
      <AnimatePresence initial={false}>
        {suggestions.map((suggestion) => (
          <motion.li
            key={suggestion.suggestion_id}
            layout
            exit={{ opacity: 0, x: 24, transition: { duration: 0.25 } }}
            className="flex items-center justify-between gap-4 border-b border-white/[0.05] pb-3"
          >
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <SourceBadge source={suggestion.source_type} />
                <span className="meta text-ink-faint">→ {suggestion.group_name}</span>
              </div>
              <p className="mt-1 truncate text-sm text-ink-soft">{suggestion.title || '(untitled)'}</p>
            </div>
            <div className="flex shrink-0 gap-1.5">
              <button
                type="button"
                onClick={() => onDecide(suggestion.suggestion_id, true)}
                className="btn-ghost p-2"
                aria-label={`Keep in ${suggestion.group_name}`}
                title="Keep"
              >
                <Check size={14} aria-hidden="true" className="text-moss" />
              </button>
              <button
                type="button"
                onClick={() => onDecide(suggestion.suggestion_id, false)}
                className="btn-ghost p-2"
                aria-label={`Remove from ${suggestion.group_name}`}
                title="Remove"
              >
                <X size={14} aria-hidden="true" className="text-youtube" />
              </button>
            </div>
          </motion.li>
        ))}
      </AnimatePresence>
    </ul>
  )
}

/* ── Page ───────────────────────────────────────────────────────────────── */
export default function PatternsPage() {
  const [summary, setSummary] = useState<DailySummary | null>(null)
  const [cells, setCells] = useState<HeatmapCell[] | null>(null)
  const [groups, setGroups] = useState<UserGroup[]>([])
  const [insight, setInsight] = useState<WeeklyInsight | null>(null)
  const [reviewQueue, setReviewQueue] = useState<GroupSuggestion[]>([])

  useEffect(() => {
    let cancelled = false
    void fetchDaily().then((data) => !cancelled && setSummary(data))
    void fetchHeatmap(7).then((data) => !cancelled && setCells(data))
    void fetchGroups().then((data) => !cancelled && setGroups(data))
    void fetchWeeklyInsight().then((data) => !cancelled && setInsight(data))
    void fetchReviewQueue().then((data) => !cancelled && setReviewQueue(data))
    return () => {
      cancelled = true
    }
  }, [])

  function handleDecide(suggestionId: string, accept: boolean) {
    setReviewQueue((current) => current.filter((s) => s.suggestion_id !== suggestionId))
    void decideSuggestion(suggestionId, accept).then(() => {
      // Member counts may have changed — refresh the group list quietly.
      void fetchGroups().then(setGroups)
    })
  }

  async function handleCreateGroup(name: string) {
    const created = await createGroup(name)
    setGroups((current) => [created, ...current])
  }

  const stats = summary
    ? [
        { label: 'time today', value: summary.total_seconds, format: formatDuration },
        { label: 'sessions', value: summary.sessions.session_count, format: (v: number) => String(Math.round(v)) },
        { label: 'avg session', value: summary.sessions.avg_session_minutes, format: (v: number) => `${Math.round(v)}m` },
        { label: 'shorts time', value: summary.shorts.shorts_seconds, format: formatDuration },
      ]
    : []

  return (
    <div>
      <PageTitle eyebrow="patterns" title="Where the time went." aside={summary ? <p className="meta">{summary.day}</p> : undefined} />

      {/* Stat row — numbers count up under the lamp */}
      <motion.div
        className="mt-10 grid grid-cols-2 gap-px overflow-hidden rounded-card border border-white/[0.06] bg-white/[0.04] sm:grid-cols-4"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5, ease: EASE_OUT, delay: 0.2 }}
      >
        {stats.map((stat) => (
          <div key={stat.label} className="bg-night-850 px-5 py-5">
            <p className="meta">{stat.label}</p>
            <p className="mt-2 font-display text-3xl text-ink" style={{ fontVariationSettings: '"opsz" 40' }}>
              <Counter value={stat.value} format={stat.format} />
            </p>
          </div>
        ))}
        {!summary &&
          Array.from({ length: 4 }, (_, index) => <div key={index} className="h-24 animate-pulse bg-night-850" />)}
      </motion.div>

      <div className="mt-8 grid grid-cols-1 gap-8 xl:grid-cols-[1.2fr_1fr]">
        {/* Left column */}
        <div className="space-y-8">
          <section className="card p-6" aria-labelledby="heatmap-heading">
            <h2 id="heatmap-heading" className="meta mb-5">
              the week, hour by hour
            </h2>
            {cells ? <Heatmap cells={cells} /> : <div className="h-36 animate-pulse rounded bg-night-800/60" />}
            <p className="meta mt-3 text-[9px] text-ink-faint">brighter = more time · red tick = a regret you marked</p>
          </section>

          {insight && (
            <motion.section
              className="card border-ember-500/15 p-6"
              initial={{ opacity: 0, y: 14 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ duration: 0.6, ease: EASE_OUT }}
              aria-labelledby="insight-heading"
            >
              <h2 id="insight-heading" className="meta mb-4">
                this week, reflected · {insight.aggregates.week_start} → {insight.aggregates.week_end}
              </h2>
              <p className="max-w-[62ch] font-display text-lg italic leading-relaxed text-ink-soft" style={{ fontVariationSettings: '"opsz" 24' }}>
                “{insight.reflection}”
              </p>
              <p className="meta mt-4 text-[9px] text-ink-faint">
                {insight.generated_by === 'llm' ? 'synthesized from aggregated numbers only — never your content' : 'computed locally'}
              </p>
            </motion.section>
          )}
        </div>

        {/* Right column */}
        <div className="space-y-8">
          <section className="card p-6" aria-labelledby="categories-heading">
            <h2 id="categories-heading" className="meta mb-5">
              time by category
            </h2>
            {summary ? <CategoryBars summary={summary} /> : <div className="h-40 animate-pulse rounded bg-night-800/60" />}
          </section>

          <section className="card p-6" aria-labelledby="groups-heading">
            <h2 id="groups-heading" className="meta mb-5">
              your goal groups
            </h2>
            <Groups groups={groups} onCreate={handleCreateGroup} />
          </section>

          <section className="card p-6" aria-labelledby="review-heading">
            <h2 id="review-heading" className="meta mb-5">
              the sunday review{reviewQueue.length > 0 ? ` · ${reviewQueue.length} waiting` : ''}
            </h2>
            <p className="mb-4 text-xs leading-5 text-ink-mute">
              Echo sorted these into your groups automatically — nothing sticks without your say.
            </p>
            <ReviewQueue suggestions={reviewQueue} onDecide={handleDecide} />
          </section>
        </div>
      </div>
    </div>
  )
}
