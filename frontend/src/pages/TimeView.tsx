import { useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import { motion } from 'framer-motion'
import { ChevronDown, ChevronLeft, ChevronRight, Clock3, Film, Layers, Loader2, Timer, X } from 'lucide-react'
import Counter from '../components/Counter'
import StatCard from '../components/StatCard'
import SourceBadge from '../components/SourceBadge'
import {
  fetchCategoryItems,
  fetchDaily,
  fetchDaySessions,
  fetchHeatmap,
  fetchSessionItems,
  fetchWeeklyInsight,
} from '../lib/api'
import { DOW_LABELS, HOUR_LABELS, asUtc, formatDuration, isoDayLabel, shiftIsoDate, todayIso } from '../lib/format'
import type {
  CategoryItem,
  DailySummary,
  HeatmapCell,
  SessionItem,
  SessionRow,
  SourceType,
  WeeklyInsight,
} from '../lib/types'

const EASE_OUT = [0.22, 1, 0.36, 1] as const

const GROUP_ORDER = ['study', 'work', 'entertainment', 'personal', 'misc']

// A pause longer than this ends a session (wba/wellbeing_analytics.py SESSION_GAP_MINUTES).
const SESSION_GAP_MINUTES = 20

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

function CategoryItemRow({ item, at }: { item: CategoryItem; at?: string }) {
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
      {at && <span className="hidden shrink-0 font-mono text-[11px] text-ink-faint sm:block">{at}</span>}
      <span className="w-12 shrink-0 text-right font-mono text-xs text-ink-mute">{itemTime(item.total_seconds)}</span>
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
              className="group grid w-full grid-cols-[1fr_auto] items-center gap-x-4 gap-y-1.5 text-left disabled:cursor-default md:grid-cols-[11rem_1fr_9rem]"
            >
              <>
                <span className="flex items-center gap-1.5 text-sm capitalize text-ink-soft transition group-enabled:group-hover:text-ink md:col-start-1 md:row-start-1">
                  <ChevronDown
                    size={14}
                    aria-hidden="true"
                    className={`shrink-0 transition-transform duration-200 ${isOpen ? 'rotate-0' : '-rotate-90'} ${
                      isEmpty ? 'text-ink-faint opacity-40' : 'text-ember-400'
                    }`}
                  />
                  {group.group_name}
                </span>
                <span className="font-mono text-xs text-ink-mute md:col-start-3 md:row-start-1 md:text-right">
                  {formatDuration(group.total_seconds)} · {percent}%
                </span>
                <span className="col-span-2 block h-[3px] rounded-full bg-night-700/60 md:col-span-1 md:col-start-2 md:row-start-1">
                <motion.span
                  className="block h-full rounded-full bg-gradient-to-r from-ember-600 to-ember-400"
                  initial={{ width: '0%' }}
                  whileInView={{ width: `${(group.total_seconds / max) * 100}%` }}
                  viewport={{ once: true }}
                  transition={{ duration: 0.9, ease: EASE_OUT, delay: 0.15 + index * 0.07 }}
                />
                </span>
              </>
            </button>
            {isOpen && <CategoryItemsPanel id={panelId} state={loaded[group.group_name]} />}
          </li>
        )
      })}
    </ul>
  )
}

/* ── Sessions of the day — a list, each opening to the items inside ─────── */
type SessionsState = { status: 'loading' } | { status: 'error' } | { status: 'ready'; sessions: SessionRow[] }
type SessionItemsState = { status: 'loading' } | { status: 'error' } | { status: 'ready'; items: SessionItem[] }

const SOURCE_NAME: Record<SourceType, string> = { gmail: 'Gmail', chrome: 'Chrome', youtube: 'YouTube' }

/** The clock time of a naive-UTC stamp, in the user's own time zone ("8:40 PM"). */
function clockTime(iso: string): string {
  const date = new Date(asUtc(iso) ?? iso)
  return Number.isNaN(date.getTime()) ? '' : date.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' })
}

/** "Chrome → YouTube → Chrome": the order of sources inside a session, or null when it stayed in one. */
function sourceFlow(items: SessionItem[]): string | null {
  const names: string[] = []
  for (const item of items) {
    const name = SOURCE_NAME[item.source_type]
    if (names[names.length - 1] !== name) names.push(name)
  }
  return names.length > 1 ? names.join(' → ') : null
}

function SessionItems({ id, state }: { id: string; state: SessionItemsState | undefined }) {
  let body: ReactNode
  if (!state || state.status === 'loading') {
    body = <div className="h-14 animate-pulse rounded bg-night-800/60" role="status" aria-label="Loading the items" />
  } else if (state.status === 'error') {
    body = <p className="py-2 text-sm text-ink-mute">Couldn't load these items. Close the session and open it again to retry.</p>
  } else if (state.items.length === 0) {
    body = <p className="py-2 text-sm text-ink-mute">None of this session's items are left.</p>
  } else {
    const flow = sourceFlow(state.items)
    body = (
      <>
        {flow && <p className="meta pb-1 pt-2 text-ink-faint">in this order · {flow}</p>}
        <ul className="divide-y divide-white/[0.05]">
          {state.items.map((item) => (
            <CategoryItemRow key={item.memory_id} item={item} at={clockTime(item.activity_at)} />
          ))}
        </ul>
      </>
    )
  }
  return (
    <motion.div
      id={id}
      initial={{ opacity: 0, y: -4 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.2, ease: EASE_OUT }}
      className="mb-3 rounded-card border border-white/[0.06] bg-night-900/60 px-4 pb-1"
    >
      {body}
    </motion.div>
  )
}

function SessionList({ day, dayText, onClose }: { day: string; dayText: string; onClose: () => void }) {
  const [state, setState] = useState<SessionsState>({ status: 'loading' })
  const [open, setOpen] = useState<ReadonlySet<string>>(new Set())
  const [loaded, setLoaded] = useState<Record<string, SessionItemsState>>({})

  // One load per day: the panel is keyed by the day, so a new day mounts a fresh one.
  useEffect(() => {
    let cancelled = false
    fetchDaySessions(day)
      .then((sessions) => !cancelled && setState({ status: 'ready', sessions }))
      .catch(() => !cancelled && setState({ status: 'error' }))
    return () => {
      cancelled = true
    }
  }, [day])

  function toggle(sessionId: string) {
    const wasOpen = open.has(sessionId)
    setOpen((current) => {
      const next = new Set(current)
      if (wasOpen) next.delete(sessionId)
      else next.add(sessionId)
      return next
    })
    // Closing never fetches; an open or finished load is reused; a failed one is retried.
    const known = loaded[sessionId]
    if (wasOpen || known?.status === 'loading' || known?.status === 'ready') return
    setLoaded((current) => ({ ...current, [sessionId]: { status: 'loading' } }))
    fetchSessionItems(sessionId)
      .then((items) => setLoaded((current) => ({ ...current, [sessionId]: { status: 'ready', items } })))
      .catch(() => setLoaded((current) => ({ ...current, [sessionId]: { status: 'error' } })))
  }

  let body: ReactNode
  if (state.status === 'loading') {
    body = (
      <div className="space-y-3" role="status" aria-label="Loading the sessions">
        {[0, 1, 2].map((index) => (
          <div key={index} className="h-11 animate-pulse rounded bg-night-800/60" />
        ))}
      </div>
    )
  } else if (state.status === 'error') {
    body = <p className="text-sm text-ink-mute">Couldn't load the sessions. Close this and open it again to retry.</p>
  } else if (state.sessions.length === 0) {
    body = <p className="text-sm text-ink-mute">No sessions on this day.</p>
  } else {
    const longest = Math.max(...state.sessions.map((session) => session.total_duration_seconds), 1)
    body = (
      <ul className="divide-y divide-white/[0.05]">
        {state.sessions.map((session) => {
          const isOpen = open.has(session.session_id)
          const panelId = `session-items-${session.session_id}`
          const share = Math.max(4, (session.total_duration_seconds / longest) * 100)
          const itemCount = session.item_count ?? 0
          return (
            <li key={session.session_id}>
              <button
                type="button"
                onClick={() => toggle(session.session_id)}
                aria-expanded={isOpen}
                aria-controls={isOpen ? panelId : undefined}
                title={isOpen ? 'Hide the items' : 'Show the items'}
                className="group grid w-full grid-cols-[1fr_auto] items-center gap-x-4 gap-y-2 py-3.5 text-left md:grid-cols-[7.5rem_1fr_4.5rem_13rem]"
              >
                <span className="flex items-center gap-1.5 font-mono text-sm text-ink transition group-hover:text-ember-300 md:col-start-1 md:row-start-1">
                  <ChevronDown
                    size={14}
                    aria-hidden="true"
                    className={`shrink-0 text-ember-400 transition-transform duration-200 ${isOpen ? 'rotate-0' : '-rotate-90'}`}
                  />
                  {clockTime(session.session_start)}
                </span>
                <span className="font-mono text-xs text-ink-soft md:col-start-3 md:row-start-1 md:text-right">
                  {itemTime(session.total_duration_seconds)}
                </span>
                <span className="col-span-2 block h-[3px] rounded-full bg-night-700/60 md:col-span-1 md:col-start-2 md:row-start-1">
                  <span
                    className="block h-full rounded-full bg-gradient-to-r from-ember-600 to-ember-400"
                    style={{ width: `${share}%` }}
                  />
                </span>
                <span className="col-span-2 flex items-center gap-2 md:col-span-1 md:col-start-4 md:row-start-1">
                  {session.dominant_group && (
                    <span className="rounded-pill border border-white/[0.09] px-2.5 py-0.5 font-mono text-[10px] capitalize text-ink-mute">
                      {session.dominant_group}
                    </span>
                  )}
                  <span className="font-mono text-[11px] text-ink-faint">
                    {itemCount} {itemCount === 1 ? 'item' : 'items'}
                  </span>
                </span>
              </button>
              {isOpen && <SessionItems id={panelId} state={loaded[session.session_id]} />}
            </li>
          )
        })}
      </ul>
    )
  }

  return (
    <motion.section
      id="sessions-panel"
      className="card mt-4 p-6"
      aria-labelledby="sessions-heading"
      initial={{ opacity: 0, y: -6 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.25, ease: EASE_OUT }}
    >
      <div className="mb-1 flex items-start justify-between gap-4">
        <h2 id="sessions-heading" className="meta">
          sessions · {dayText}
          {state.status === 'ready' && state.sessions.length > 0 ? ` · ${state.sessions.length}` : ''}
        </h2>
        <button
          type="button"
          onClick={onClose}
          aria-label="Close the sessions"
          className="-mr-1 -mt-1 rounded-full p-1.5 text-ink-faint transition hover:text-ink-soft"
        >
          <X size={14} aria-hidden="true" />
        </button>
      </div>
      <p className="mb-3 max-w-[60ch] text-xs leading-5 text-ink-mute">
        A session is a run of activity with no pause longer than {SESSION_GAP_MINUTES} minutes. The time on each row is
        what you spent on the items in it; the bar compares it with the day's longest.
      </p>
      {body}
    </motion.section>
  )
}

/* ── Hour × day heatmap — lamplight intensity, flag ticks ─────────────── */
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
              <span className="meta text-[10px]">a moment you flagged</span>
            </div>
            <span className="meta text-[10px] text-ink-faint">one square = one hour · your local time</span>
          </div>
        </div>
      </div>
    </div>
  )
}


/** Stands in for the weekly reflection while it is written (one LLM call, a few seconds). */
function InsightLoading({ weekText }: { weekText: string }) {
  return (
    <section className="card border-ember-500/15 p-6" role="status" aria-live="polite" aria-labelledby="insight-loading-heading">
      <h2 id="insight-loading-heading" className="meta mb-4">
        this week, reflected · {weekText}
      </h2>
      <div className="flex items-center gap-3">
        <Loader2 size={18} aria-hidden="true" className="shrink-0 text-ember-400 motion-safe:animate-spin" />
        <p className="font-display text-lg italic text-ink-soft" style={{ fontVariationSettings: '"opsz" 24' }}>
          Your weekly insights are loading…
        </p>
      </div>
      <p className="mt-2 pl-[30px] text-xs leading-5 text-ink-mute">Echo is writing a short note from this week’s numbers.</p>
      <div className="mt-5 space-y-2.5" aria-hidden="true">
        <div className="h-3 w-full animate-pulse rounded bg-night-800/70" />
        <div className="h-3 w-5/6 animate-pulse rounded bg-night-800/70" />
        <div className="h-3 w-2/3 animate-pulse rounded bg-night-800/70" />
      </div>
    </section>
  )
}

/**
 * The "Time" view of Patterns: the day's numbers, the week's heatmap and weekly reflection, and the
 * categories behind the day. The page around it (title, view switch) is `PatternsPage`.
 */
export default function TimeView() {
  const today = todayIso()
  // D6.5: the day the stats, sessions and category card show, and the last day of the week the
  // heatmap and the reflection cover. Both are local days, YYYY-MM-DD; never after today.
  const [day, setDay] = useState(today)
  const [weekEnd, setWeekEnd] = useState(today)
  const [summary, setSummary] = useState<DailySummary | null>(null)
  const [cells, setCells] = useState<HeatmapCell[] | null>(null)
  const [insight, setInsight] = useState<WeeklyInsight | null>(null)
  const [insightLoading, setInsightLoading] = useState(true)
  // The "sessions" and "avg session" cards open the day's sessions under the stat row.
  const [sessionsOpen, setSessionsOpen] = useState(false)

  // The chosen day: the stat row, the sessions and the category card.
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

  const dayText = isoDayLabel(day)
  const weekStart = shiftIsoDate(weekEnd, -6)
  const weekText = weekEnd === today ? 'the last 7 days' : `${isoDayLabel(weekStart)} → ${isoDayLabel(weekEnd)}`

  // Only a day with sessions has anything to open; the panel closes itself on a day without any.
  const sessionCount = summary?.sessions.session_count ?? null
  const canOpenSessions = sessionCount !== null && sessionCount > 0
  const showSessions = sessionsOpen && (sessionCount === null || sessionCount > 0)
  const sessionsAction = canOpenSessions
    ? {
        hint: 'see the sessions',
        hideHint: 'hide the sessions',
        open: showSessions,
        controls: 'sessions-panel',
        onToggle: () => setSessionsOpen((current) => !current),
      }
    : undefined

  return (
    <div>
      {/* ── The day: its stepper sits right above the numbers it drives ── */}
      <div className="flex flex-wrap items-center gap-x-4 gap-y-3">
        <PeriodStepper label="day" value={dayText} onShift={shiftDay} atToday={day >= today} />
        {day !== today && (
          <button type="button" onClick={() => setDay(today)} className="btn-ghost font-mono text-xs normal-case">
            back to today
          </button>
        )}
      </div>

      <motion.div
        className="mt-5 grid grid-cols-2 gap-3 xl:grid-cols-4"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5, ease: EASE_OUT, delay: 0.1 }}
      >
        {summary ? (
          <>
            <StatCard
              icon={Clock3}
              label={day === today ? 'time today' : `time · ${dayText}`}
              value={<Counter value={summary.total_seconds} format={formatDuration} />}
            />
            <StatCard
              icon={Layers}
              label="sessions"
              value={<Counter value={summary.sessions.session_count} format={(v: number) => String(Math.round(v))} />}
              action={sessionsAction}
            />
            <StatCard
              icon={Timer}
              label="avg session"
              value={<Counter value={summary.sessions.avg_session_minutes} format={(v: number) => `${Math.round(v)}m`} />}
              action={sessionsAction}
            />
            <StatCard
              icon={Film}
              label="shorts time"
              value={<Counter value={summary.shorts.shorts_seconds} format={formatDuration} />}
            />
          </>
        ) : (
          Array.from({ length: 4 }, (_, index) => (
            <div key={index} className="h-[124px] animate-pulse rounded-card bg-night-850" aria-hidden="true" />
          ))
        )}
      </motion.div>

      {showSessions && <SessionList key={day} day={day} dayText={dayText} onClose={() => setSessionsOpen(false)} />}

      <section className="card mt-6 p-6" aria-labelledby="categories-heading">
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

      {/* ── The week: its stepper sits right above the heatmap and the reflection it drives ── */}
      <div className="mt-12 flex flex-wrap items-center gap-x-4 gap-y-3 border-t border-white/[0.06] pt-8">
        <PeriodStepper label="week" value={weekText} onShift={shiftWeek} atToday={weekEnd >= today} />
        {weekEnd !== today && (
          <button type="button" onClick={() => setWeekEnd(today)} className="btn-ghost font-mono text-xs normal-case">
            back to today
          </button>
        )}
      </div>

      {/* The heatmap gets the full width: 24 hour squares need more than half a screen to stay readable */}
      <section className="card mt-5 p-6" aria-labelledby="heatmap-heading">
        <h2 id="heatmap-heading" className="meta mb-5">
          the week, hour by hour · {weekText}
        </h2>
        {cells ? <Heatmap cells={cells} weekEnd={weekEnd} /> : <div className="h-36 animate-pulse rounded bg-night-800/60" />}
      </section>

      <div className="mt-6">
        {insightLoading && !insight && <InsightLoading weekText={weekText} />}
        {insight && (
          <motion.section
            className={`card border-ember-500/15 p-6 transition-opacity ${insightLoading ? 'opacity-50' : ''}`}
            initial={{ opacity: 0, y: 14 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.6, ease: EASE_OUT }}
            aria-labelledby="insight-heading"
          >
            <h2 id="insight-heading" className="meta mb-4 flex flex-wrap items-center justify-between gap-x-4 gap-y-1">
              <span>
                this week, reflected · {insight.aggregates.week_start} → {insight.aggregates.week_end}
              </span>
              {/* The old note stays (dimmed) while the next week's is written, so say that it is changing */}
              {insightLoading && (
                <span role="status" className="inline-flex items-center gap-1.5 text-ember-300">
                  <Loader2 size={12} aria-hidden="true" className="motion-safe:animate-spin" />
                  loading this week’s insights…
                </span>
              )}
            </h2>
            <p className="max-w-[68ch] font-display text-lg italic leading-relaxed text-ink-soft" style={{ fontVariationSettings: '"opsz" 24' }}>
              “{insight.reflection}”
            </p>
            <p className="meta mt-4 text-[9px] text-ink-faint">
              {insight.generated_by === 'llm' ? 'synthesized from aggregated numbers only — never your content' : 'computed locally'}
            </p>
          </motion.section>
        )}
      </div>
    </div>
  )
}
