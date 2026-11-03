import { useEffect, useMemo, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { Link } from 'react-router-dom'
import { Check, ChevronDown, ChevronLeft, ChevronRight, Plus, X } from 'lucide-react'
import PageTitle from '../components/PageTitle'
import Counter from '../components/Counter'
import SourceBadge from '../components/SourceBadge'
import {
  createGroup,
  decideSuggestion,
  fetchCategoryItems,
  fetchDaily,
  fetchGroups,
  fetchHeatmap,
  fetchReviewQueue,
  fetchWeeklyInsight,
} from '../lib/api'
import { DOW_LABELS, HOUR_LABELS, formatDuration, isoDayLabel, shiftIsoDate, todayIso } from '../lib/format'
import type { CategoryItem, DailySummary, GroupSuggestion, HeatmapCell, UserGroup, WeeklyInsight } from '../lib/types'

const EASE_OUT = [0.22, 1, 0.36, 1] as const

const GROUP_ORDER = ['study', 'work', 'entertainment', 'personal', 'misc']

/* ── Period stepper — previous / next day or week (D6.5) ────────────────── */
function PeriodStepper({
  label,
  value,
  onShift,
  atToday,
}: {
  label: string
  value: string
  onShift: (direction: -1 | 1) => void
  atToday: boolean
}) {
  return (
    <div
      className="flex items-center gap-2 rounded-pill border border-ember-500/25 bg-night-850 py-1.5 pl-4 pr-1.5 shadow-raise"
      role="group"
      aria-label={`Choose the ${label}`}
    >
      <span className="meta mr-1 text-ember-400">{label}</span>
      <button
        type="button"
        onClick={() => onShift(-1)}
        className="flex h-8 w-8 items-center justify-center rounded-full border border-ember-500/30 bg-ember-500/10 text-ember-400 transition duration-200 ease-out hover:border-ember-400/60 hover:bg-ember-500/20 hover:text-ember-300 active:scale-[0.94]"
        aria-label={`Previous ${label}`}
        title={`Previous ${label}`}
      >
        <ChevronLeft size={16} aria-hidden="true" />
      </button>
      <span className="min-w-[9.5rem] text-center font-mono text-sm text-ink" aria-live="polite">
        {value}
      </span>
      <button
        type="button"
        onClick={() => onShift(1)}
        disabled={atToday}
        className="flex h-8 w-8 items-center justify-center rounded-full border border-ember-500/30 bg-ember-500/10 text-ember-400 transition duration-200 ease-out hover:border-ember-400/60 hover:bg-ember-500/20 hover:text-ember-300 active:scale-[0.94] disabled:cursor-not-allowed disabled:border-white/10 disabled:bg-transparent disabled:text-ink-faint disabled:hover:bg-transparent"
        aria-label={`Next ${label}`}
        title={atToday ? 'Nothing after today' : `Next ${label}`}
      >
        <ChevronRight size={16} aria-hidden="true" />
      </button>
    </div>
  )
}

/* ── Category bars — thin, editorial, no chart library ─────────────────── */
type CategoryItemsState =
  | { status: 'loading' }
  | { status: 'error' }
  | { status: 'ready'; items: CategoryItem[] }

/** Under half a minute shows as "<1m"; formatDuration would round it to "0m". */
function itemTime(seconds: number): string {
  return seconds > 0 && seconds < 30 ? '<1m' : formatDuration(seconds)
}

function CategoryItemRow({ item }: { item: CategoryItem }) {
  const title = item.title?.trim() || 'Untitled'
  const detail = item.sender ?? item.channel_name ?? item.domain ?? null
  const titleClass = 'block truncate text-sm text-ink-soft'
  return (
    <li className="flex items-center gap-3 py-2.5">
      <div className="w-20 shrink-0">
        <SourceBadge source={item.source_type} />
      </div>
      <div className="min-w-0 flex-1">
        {item.url ? (
          <a
            href={item.url}
            target="_blank"
            rel="noopener noreferrer"
            className={`${titleClass} transition hover:text-ember-300`}
            aria-label={`${title}. Opens in a new tab.`}
          >
            {title}
          </a>
        ) : (
          <span className={titleClass}>{title}</span>
        )}
        {detail && <span className="mt-0.5 block truncate font-mono text-[11px] text-ink-faint">{detail}</span>}
      </div>
      <span className="shrink-0 font-mono text-xs text-ink-mute">{itemTime(item.total_seconds)}</span>
    </li>
  )
}

function CategoryItemsPanel({ id, state }: { id: string; state: CategoryItemsState | undefined }) {
  let body: JSX.Element
  if (!state || state.status === 'loading') {
    body = <div className="h-16 animate-pulse rounded bg-night-800/60" aria-label="Loading items" />
  } else if (state.status === 'error') {
    body = <p className="text-sm text-ink-mute">Couldn't load these items. Close and open the category to try again.</p>
  } else if (state.items.length === 0) {
    body = <p className="text-sm text-ink-mute">No items for this category.</p>
  } else {
    body = (
      <ul className="divide-y divide-white/[0.05]">
        {state.items.map((item) => (
          <CategoryItemRow key={item.memory_id} item={item} />
        ))}
      </ul>
    )
  }
  return (
    <motion.div
      id={id}
      initial={{ opacity: 0, y: -4 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.2, ease: EASE_OUT }}
      className="mt-3 rounded-card border border-white/[0.06] bg-night-900/60 px-4 py-1"
    >
      {body}
    </motion.div>
  )
}

/** Each bar opens a list of the items behind it; the list loads on the first open. */
function CategoryBars({ summary, dayText }: { summary: DailySummary; dayText: string }) {
  const [open, setOpen] = useState<ReadonlySet<string>>(new Set())
  const [loaded, setLoaded] = useState<Record<string, CategoryItemsState>>({})
  const max = Math.max(...summary.by_system_group.map((group) => group.total_seconds), 1)
  const rows = [...summary.by_system_group].sort(
    (a, b) => GROUP_ORDER.indexOf(a.group_name) - GROUP_ORDER.indexOf(b.group_name),
  )

  function toggle(groupName: string) {
    const wasOpen = open.has(groupName)
    setOpen((current) => {
      const next = new Set(current)
      if (wasOpen) next.delete(groupName)
      else next.add(groupName)
      return next
    })
    // Closing never fetches; an open or finished load is reused; a failed one is retried.
    const state = loaded[groupName]
    if (wasOpen || state?.status === 'loading' || state?.status === 'ready') return
    setLoaded((current) => ({ ...current, [groupName]: { status: 'loading' } }))
    fetchCategoryItems(summary.day, groupName)
      .then((items) => setLoaded((current) => ({ ...current, [groupName]: { status: 'ready', items } })))
      .catch(() => setLoaded((current) => ({ ...current, [groupName]: { status: 'error' } })))
  }

  return (
    <ul className="space-y-3.5" aria-label={`Time by category, ${dayText}`}>
      {rows.map((group, index) => {
        const percent = summary.total_seconds ? Math.round((group.total_seconds / summary.total_seconds) * 100) : 0
        const isOpen = open.has(group.group_name)
        const isEmpty = group.total_seconds === 0 && group.item_count === 0
        const panelId = `category-items-${group.group_name}`
        return (
          <li key={group.group_name}>
            <button
              type="button"
              onClick={() => toggle(group.group_name)}
              disabled={isEmpty}
              aria-expanded={isOpen}
              aria-controls={isOpen ? panelId : undefined}
              title={isEmpty ? 'Nothing in this category' : isOpen ? 'Hide the items' : 'Show the items'}
              className="group block w-full text-left disabled:cursor-default"
            >
              <span className="flex items-baseline justify-between gap-4">
                <span className="flex items-center gap-1.5 text-sm capitalize text-ink-soft transition group-enabled:group-hover:text-ink">
                  <ChevronDown
                    size={14}
                    aria-hidden="true"
                    className={`shrink-0 transition-transform duration-200 ${isOpen ? 'rotate-0' : '-rotate-90'} ${
                      isEmpty ? 'text-ink-faint opacity-40' : 'text-ember-400'
                    }`}
                  />
                  {group.group_name}
                </span>
                <span className="font-mono text-xs text-ink-mute">
                  {formatDuration(group.total_seconds)} · {percent}%
                </span>
              </span>
              <span className="mt-1.5 block h-[3px] rounded-full bg-night-700/60">
                <motion.span
                  className="block h-full rounded-full bg-gradient-to-r from-ember-600 to-ember-400"
                  initial={{ width: '0%' }}
                  whileInView={{ width: `${(group.total_seconds / max) * 100}%` }}
                  viewport={{ once: true }}
                  transition={{ duration: 0.9, ease: EASE_OUT, delay: 0.15 + index * 0.07 }}
                />
              </span>
            </button>
            {isOpen && <CategoryItemsPanel id={panelId} state={loaded[group.group_name]} />}
          </li>
        )
      })}
    </ul>
  )
}

/* ── Hour × day heatmap — lamplight intensity, regret ticks ─────────────── */
// Four brightness steps instead of a smooth fade: a step is easy to match against the legend.
const HEAT_ALPHA = [0.25, 0.45, 0.7, 0.95] as const
const HEAT_EMPTY = 'rgba(255,255,255,0.045)'

function heatColor(step: number): string {
  return `rgba(245, 169, 71, ${HEAT_ALPHA[step]})`
}

function hourText(hour: number): string {
  return `${hour % 12 || 12}${hour < 12 ? 'am' : 'pm'}`
}

interface HeatRow {
  iso: string
  label: string
  longLabel: string
  isToday: boolean
  totalSeconds: number
  hours: (HeatmapCell | undefined)[]
}

interface HeatPick {
  rowIndex: number
  hour: number
}

function Heatmap({ cells, weekEnd }: { cells: HeatmapCell[]; weekEnd: string }) {
  const [picked, setPicked] = useState<HeatPick | null>(null)

  // One row per calendar day, oldest first, ending on the chosen day. The backend groups by
  // weekday, and 7 consecutive days contain each weekday once, so weekday -> row is exact.
  const rows = useMemo<HeatRow[]>(() => {
    const byKey = new Map<string, HeatmapCell>()
    for (const cell of cells) byKey.set(`${cell.day_of_week}-${cell.hour}`, cell)
    const today = todayIso()
    return Array.from({ length: 7 }, (_, index) => {
      const iso = shiftIsoDate(weekEnd, index - 6)
      const [year, month, dayOfMonth] = iso.split('-').map(Number)
      const date = new Date(year, month - 1, dayOfMonth)
      const weekday = date.getDay()
      const hours = Array.from({ length: 24 }, (_, hour) => byKey.get(`${weekday}-${hour}`))
      return {
        iso,
        label: iso === today ? 'today' : `${DOW_LABELS[weekday]} ${dayOfMonth}`,
        longLabel: date.toLocaleDateString(undefined, { weekday: 'long', day: 'numeric', month: 'short' }),
        isToday: iso === today,
        totalSeconds: hours.reduce((sum, cell) => sum + (cell?.total_seconds ?? 0), 0),
        hours,
      }
    })
  }, [cells, weekEnd])

  const maxSeconds = Math.max(...cells.map((cell) => cell.total_seconds), 1)

  const busiest = useMemo<HeatPick | null>(() => {
    let best: HeatPick | null = null
    let bestSeconds = 0
    rows.forEach((row, rowIndex) =>
      row.hours.forEach((cell, hour) => {
        if (cell && cell.total_seconds > bestSeconds) {
          best = { rowIndex, hour }
          bestSeconds = cell.total_seconds
        }
      }),
    )
    return best
  }, [rows])

  const shown = picked ?? busiest
  let readout = 'No activity recorded in these days.'
  if (shown) {
    const row = rows[shown.rowIndex]
    const cell = row.hours[shown.hour]
    const when = `${row.longLabel}, ${hourText(shown.hour)}–${hourText((shown.hour + 1) % 24)}`
    if (!cell || (cell.total_seconds === 0 && cell.item_count === 0)) {
      readout = `${when} · nothing recorded`
    } else {
      const parts = [when, formatDuration(cell.total_seconds), `${cell.item_count} ${cell.item_count === 1 ? 'item' : 'items'}`]
      if (cell.has_regret) parts.push('you marked a regret here')
      readout = parts.join(' · ')
    }
    if (!picked) readout = `Busiest hour: ${readout}`
  }

  return (
    <div role="img" aria-label={`Activity heatmap, one row per day, one square per hour. ${readout}`}>
      <div className="overflow-x-auto pb-2">
        <div className="min-w-[600px]">
          <p className="mb-4 min-h-[1.25rem] font-mono text-xs text-ember-300" aria-hidden="true">
            {readout}
          </p>

          <div className="space-y-[3px]" onMouseLeave={() => setPicked(null)}>
            {rows.map((row, rowIndex) => (
              <div key={row.iso} className="flex items-center gap-2">
                <span
                  className={`w-14 shrink-0 font-mono text-[11px] uppercase ${row.isToday ? 'text-ember-400' : 'text-ink-mute'}`}
                >
                  {row.label}
                </span>
                <div className="flex flex-1 gap-[3px]">
                  {row.hours.map((cell, hour) => {
                    const hasActivity = !!cell && (cell.total_seconds > 0 || cell.item_count > 0)
                    const step = hasActivity
                      ? Math.min(HEAT_ALPHA.length - 1, Math.max(0, Math.ceil((cell.total_seconds / maxSeconds) * HEAT_ALPHA.length) - 1))
                      : -1
                    const isShown = shown?.rowIndex === rowIndex && shown.hour === hour
                    return (
                      <motion.span
                        key={hour}
                        initial={{ opacity: 0 }}
                        whileInView={{ opacity: 1 }}
                        viewport={{ once: true }}
                        transition={{ duration: 0.3, delay: (rowIndex * 24 + hour) * 0.0015 }}
                        onMouseEnter={() => setPicked({ rowIndex, hour })}
                        onClick={() => setPicked({ rowIndex, hour })}
                        className={`relative h-6 flex-1 cursor-default rounded-[4px] ${isShown ? 'ring-1 ring-ink' : ''}`}
                        style={{ backgroundColor: step < 0 ? HEAT_EMPTY : heatColor(step) }}
                      >
                        {cell?.has_regret && (
                          <span
                            aria-hidden="true"
                            className="absolute -right-0.5 -top-0.5 h-2 w-2 rounded-full bg-youtube ring-2 ring-night-850"
                          />
                        )}
                      </motion.span>
                    )
                  })}
                </div>
                <span className="w-14 shrink-0 text-right font-mono text-[11px] text-ink-soft">
                  {row.totalSeconds > 0 ? formatDuration(row.totalSeconds) : '—'}
                </span>
              </div>
            ))}
          </div>

          {/* Hour axis: a label every 3 hours, under the squares it names */}
          <div className="mt-2 flex items-center gap-2" aria-hidden="true">
            <span className="w-14 shrink-0" />
            <div className="flex flex-1 gap-[3px]">
              {HOUR_LABELS.map((text, hour) => (
                <span key={hour} className="relative h-4 flex-1">
                  {text && (
                    <span className="absolute left-1/2 -translate-x-1/2 whitespace-nowrap font-mono text-[10px] text-ink-mute">
                      {text}
                    </span>
                  )}
                </span>
              ))}
            </div>
            <span className="w-14 shrink-0 text-right font-mono text-[10px] uppercase text-ink-faint">total</span>
          </div>

          {/* Legend */}
          <div className="mt-5 flex flex-wrap items-center gap-x-6 gap-y-2" aria-hidden="true">
            <div className="flex items-center gap-1.5">
              <span className="meta mr-1 text-[10px]">less</span>
              <span className="h-3.5 w-3.5 rounded-[3px]" style={{ backgroundColor: HEAT_EMPTY }} />
              {HEAT_ALPHA.map((_, step) => (
                <span key={step} className="h-3.5 w-3.5 rounded-[3px]" style={{ backgroundColor: heatColor(step) }} />
              ))}
              <span className="meta ml-1 text-[10px]">more time</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="h-2 w-2 rounded-full bg-youtube" />
              <span className="meta text-[10px]">a regret you marked</span>
            </div>
            <span className="meta text-[10px] text-ink-faint">one square = one hour · your local time</span>
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
                {group.auto_assignment_active
                  ? 'auto-sorting on'
                  : `${Math.max(0, 5 - (group.confirmed_count ?? group.member_count))} more to auto-sort`}
              </p>
              <div className="mt-1 flex justify-end gap-3">
                <Link
                  to={`/groups/${group.group_id}/items`}
                  className="meta inline-block text-[9px] text-ember-400 transition hover:text-ember-300"
                  aria-label={`Items in ${group.group_name}`}
                >
                  items →
                </Link>
                <Link
                  to={`/groups/${group.group_id}/rules`}
                  className="meta inline-block text-[9px] text-ember-400 transition hover:text-ember-300"
                  aria-label={`Rules for ${group.group_name}`}
                >
                  rules →
                </Link>
              </div>
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
                {suggestion.decision === 'auto_accepted' && (
                  <span className="rounded-pill border border-ember-500/30 px-2 py-0.5 font-mono text-[9px] uppercase tracking-wider text-ember-300">
                    added automatically
                  </span>
                )}
              </div>
              <p className="mt-1 truncate text-sm text-ink-soft">{suggestion.title || '(untitled)'}</p>
            </div>
            <div className="flex shrink-0 gap-1.5">
              <button
                type="button"
                onClick={() => onDecide(suggestion.suggestion_id, true)}
                className="btn-ghost p-2"
                aria-label={`Keep in ${suggestion.group_name}`}
                title={suggestion.decision === 'auto_accepted' ? 'Keep' : 'Add to group'}
              >
                <Check size={14} aria-hidden="true" className="text-moss" />
              </button>
              <button
                type="button"
                onClick={() => onDecide(suggestion.suggestion_id, false)}
                className="btn-ghost p-2"
                aria-label={`Remove from ${suggestion.group_name}`}
                title={suggestion.decision === 'auto_accepted' ? 'Remove' : "Don't add"}
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
  const today = todayIso()
  // D6.5: the day the stats and category card show, and the last day of the week the
  // heatmap and the reflection cover. Both are local days, YYYY-MM-DD; never after today.
  const [day, setDay] = useState(today)
  const [weekEnd, setWeekEnd] = useState(today)
  const [summary, setSummary] = useState<DailySummary | null>(null)
  const [cells, setCells] = useState<HeatmapCell[] | null>(null)
  const [groups, setGroups] = useState<UserGroup[]>([])
  const [insight, setInsight] = useState<WeeklyInsight | null>(null)
  const [insightLoading, setInsightLoading] = useState(true)
  const [reviewQueue, setReviewQueue] = useState<GroupSuggestion[]>([])

  useEffect(() => {
    let cancelled = false
    void fetchGroups().then((data) => !cancelled && setGroups(data))
    void fetchReviewQueue().then((data) => !cancelled && setReviewQueue(data))
    return () => {
      cancelled = true
    }
  }, [])

  // The chosen day: the stat row and the category card.
  useEffect(() => {
    let cancelled = false
    setSummary(null)
    void fetchDaily(day).then((data) => !cancelled && setSummary(data))
    return () => {
      cancelled = true
    }
  }, [day])

  // The chosen week: the heatmap and the reflection (7 days ending on weekEnd).
  useEffect(() => {
    let cancelled = false
    setCells(null)
    setInsightLoading(true)
    void fetchHeatmap(7, weekEnd).then((data) => !cancelled && setCells(data))
    void fetchWeeklyInsight(weekEnd)
      .then((data) => !cancelled && setInsight(data))
      .finally(() => !cancelled && setInsightLoading(false))
    return () => {
      cancelled = true
    }
  }, [weekEnd])

  /** Move by one step, never past today. */
  function shiftDay(direction: -1 | 1) {
    setDay((current) => {
      const next = shiftIsoDate(current, direction)
      return next > today ? today : next
    })
  }

  function shiftWeek(direction: -1 | 1) {
    setWeekEnd((current) => {
      const next = shiftIsoDate(current, direction * 7)
      return next > today ? today : next
    })
  }

  function backToToday() {
    setDay(today)
    setWeekEnd(today)
  }

  const dayText = isoDayLabel(day)
  const weekStart = shiftIsoDate(weekEnd, -6)
  const weekText = weekEnd === today ? 'the last 7 days' : `${isoDayLabel(weekStart)} → ${isoDayLabel(weekEnd)}`

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
        { label: day === today ? 'time today' : `time · ${dayText}`, value: summary.total_seconds, format: formatDuration },
        { label: 'sessions', value: summary.sessions.session_count, format: (v: number) => String(Math.round(v)) },
        { label: 'avg session', value: summary.sessions.avg_session_minutes, format: (v: number) => `${Math.round(v)}m` },
        { label: 'shorts time', value: summary.shorts.shorts_seconds, format: formatDuration },
      ]
    : []

  return (
    <div>
      <PageTitle eyebrow="patterns" title="Where the time went." aside={<p className="meta">{day}</p>} />

      {/* Period controls (D6.5): the day drives the stats and categories, the week the heatmap and reflection */}
      <div className="mt-8 flex flex-wrap items-center gap-x-8 gap-y-3">
        <PeriodStepper label="day" value={dayText} onShift={shiftDay} atToday={day >= today} />
        <PeriodStepper label="week" value={weekText} onShift={shiftWeek} atToday={weekEnd >= today} />
        {(day !== today || weekEnd !== today) && (
          <button type="button" onClick={backToToday} className="btn-ghost font-mono text-xs normal-case">
            back to today
          </button>
        )}
      </div>

      {/* Stat row — numbers count up under the lamp */}
      <motion.div
        className="mt-6 grid grid-cols-2 gap-px overflow-hidden rounded-card border border-white/[0.06] bg-white/[0.04] sm:grid-cols-4"
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

      {/* The heatmap gets the full width: 24 hour squares need more than half a screen to stay readable */}
      <section className="card mt-8 p-6" aria-labelledby="heatmap-heading">
        <h2 id="heatmap-heading" className="meta mb-5">
          the week, hour by hour · {weekText}
        </h2>
        {cells ? <Heatmap cells={cells} weekEnd={weekEnd} /> : <div className="h-36 animate-pulse rounded bg-night-800/60" />}
      </section>

      <div className="mt-8 grid grid-cols-1 items-start gap-8 xl:grid-cols-[1.2fr_1fr]">
        {/* Left column */}
        <div className="space-y-8">
          {insightLoading && !insight && <div className="card h-36 animate-pulse bg-night-850/60" aria-hidden="true" />}
          {insight && (
            <motion.section
              className={`card border-ember-500/15 p-6 transition-opacity ${insightLoading ? 'opacity-50' : ''}`}
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

          <section className="card p-6" aria-labelledby="categories-heading">
            {/* D6.6: the card names its day, so it never reads as the week's numbers */}
            <h2 id="categories-heading" className="meta mb-5">
              time by category · {dayText}
            </h2>
            {summary ? (
              summary.total_seconds > 0 ? (
                <CategoryBars key={summary.day} summary={summary} dayText={dayText} />
              ) : (
                <p className="text-sm text-ink-mute">No time recorded {day === today ? 'yet today' : 'on this day'}.</p>
              )
            ) : (
              <div className="h-40 animate-pulse rounded bg-night-800/60" />
            )}
          </section>
        </div>

        {/* Right column */}
        <div className="space-y-8">
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
              Echo never decides alone when it is unsure. If a rule you wrote and the similarity to items you
              confirmed both agree, the item is added and shown here, where one click undoes it. Anything less
              certain waits for your yes. Only items you confirmed ever teach Echo.
            </p>
            <ReviewQueue suggestions={reviewQueue} onDecide={handleDecide} />
          </section>
        </div>
      </div>
    </div>
  )
}
