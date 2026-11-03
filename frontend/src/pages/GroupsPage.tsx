import { useEffect, useState, type ReactNode } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { Link, useLocation } from 'react-router-dom'
import { Check, ChevronDown, ListChecks, Plus, RefreshCw, SlidersHorizontal, Target, Trash2, X } from 'lucide-react'
import PageTitle from '../components/PageTitle'
import SourceBadge from '../components/SourceBadge'
import {
  decideSuggestion,
  deleteGroup,
  fetchGroupTime,
  fetchGroupTimes,
  fetchGroups,
  fetchReviewQueue,
  refreshReviewQueue,
  runAutoAssignment,
  type AutoAssignResult,
} from '../lib/api'
import { formatDuration } from '../lib/format'
import type { GroupSuggestion, GroupTimedItem, UserGroup } from '../lib/types'

const EASE_OUT = [0.22, 1, 0.36, 1] as const

// A group starts sorting by itself after this many confirmed items (the progress bar fills towards it).
const AUTO_SORT_AFTER = 5
const NEW_GROUP_CLASS =
  'inline-flex items-center gap-2.5 rounded-pill bg-ember-500 px-6 py-3 text-base font-semibold text-night-950 transition duration-200 ease-out hover:bg-ember-400 active:scale-[0.97]'
const CARD_BUTTON = 'btn-ghost inline-flex items-center gap-2 px-4 py-2 text-sm'

/** Time spent, in words: "no time yet", "<1m" under half a minute, else "2h 15m". `undefined` is still loading. */
function spentText(seconds: number | undefined): string {
  if (seconds === undefined) return '…'
  if (seconds <= 0) return 'no time yet'
  return seconds < 30 ? '<1m' : formatDuration(seconds)
}

type GroupDetail =
  | { status: 'loading' }
  | { status: 'error' }
  | { status: 'ready'; total: number; items: GroupTimedItem[] }

/** What is inside one goal group: its items, most time first, and the total. */
function GroupContents({ id, detail }: { id: string; detail: GroupDetail }) {
  let body: ReactNode
  if (detail.status === 'loading') {
    body = <div className="my-3 h-20 animate-pulse rounded bg-night-800/60" role="status" aria-label="Loading the items" />
  } else if (detail.status === 'error') {
    body = <p className="py-3 text-[15px] text-ink-mute">Couldn’t load the items. Close this and open it again to retry.</p>
  } else if (detail.items.length === 0) {
    body = <p className="py-3 text-[15px] text-ink-mute">No items in this group yet.</p>
  } else {
    body = (
      <>
        <ul className="divide-y divide-white/[0.05]">
          {detail.items.map((item) => {
            const title = item.title?.trim() || 'Untitled'
            const place = item.sender ?? item.channel_name ?? item.domain ?? null
            return (
              <li key={item.memory_id} className="flex items-center gap-3 py-3">
                <div className="w-20 shrink-0">
                  <SourceBadge source={item.source_type} />
                </div>
                <div className="min-w-0 flex-1">
                  {item.url ? (
                    <a
                      href={item.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="block truncate text-base text-ink-soft transition hover:text-ember-300"
                      aria-label={`${title}. Opens in a new tab.`}
                    >
                      {title}
                    </a>
                  ) : (
                    <span className="block truncate text-base text-ink-soft">{title}</span>
                  )}
                  {place && <span className="block truncate font-mono text-xs text-ink-faint">{place}</span>}
                </div>
                <span className="shrink-0 font-mono text-sm text-ink-mute">{spentText(item.total_seconds)}</span>
              </li>
            )
          })}
        </ul>
        <p className="flex items-center justify-between gap-4 border-t border-white/[0.06] py-3 text-[15px] text-ink-soft">
          <span>
            Total across {detail.items.length} {detail.items.length === 1 ? 'item' : 'items'}
          </span>
          <span className="font-mono text-base text-ember-300">{spentText(detail.total)}</span>
        </p>
      </>
    )
  }
  return (
    <motion.div
      id={id}
      initial={{ opacity: 0, y: -4 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.2, ease: EASE_OUT }}
      className="mt-3 rounded-card border border-white/[0.06] bg-night-900/60 px-4"
    >
      {body}
    </motion.div>
  )
}

/* ── One goal group, as a card ──────────────────────────────────────────── */
function GroupCard({
  group,
  seconds,
  waiting,
  onDelete,
}: {
  group: UserGroup
  /** All-time seconds spent on the group's items; undefined while it loads. */
  seconds: number | undefined
  /** Items of this group in the Sunday review (suggestions to answer, or automatic additions to look at). */
  waiting: number
  onDelete: (groupId: string) => Promise<void>
}) {
  const [open, setOpen] = useState(false)
  const [detail, setDetail] = useState<GroupDetail | null>(null)
  const [confirming, setConfirming] = useState(false)
  const [deleting, setDeleting] = useState(false)
  const [failed, setFailed] = useState(false)

  const confirmed = group.confirmed_count ?? group.member_count
  const progress = group.auto_assignment_active ? 1 : Math.min(1, confirmed / AUTO_SORT_AFTER)
  const missing = Math.max(0, AUTO_SORT_AFTER - confirmed)
  // Three states, each saying what is true now. "Ready" is not an activity: matching runs only when the
  // review's "Check recent items now" is pressed (or a scheduler calls it), never continuously.
  const state: 'waiting' | 'ready' | 'not-ready' = !group.auto_assignment_active ? 'not-ready' : waiting > 0 ? 'waiting' : 'ready'
  const status =
    state === 'waiting'
      ? `${waiting} ${waiting === 1 ? 'item is' : 'items are'} waiting in the review`
      : state === 'ready'
        ? 'Ready. Echo suggests matches when it checks.'
        : `${missing} more ${missing === 1 ? 'example' : 'examples'} and Echo can start suggesting`

  // The items load the first time the card opens; a failed load is tried again on the next open.
  function toggleContents() {
    const wasOpen = open
    setOpen(!wasOpen)
    if (wasOpen || detail?.status === 'loading' || detail?.status === 'ready') return
    setDetail({ status: 'loading' })
    fetchGroupTime(group.group_id)
      .then((data) => setDetail({ status: 'ready', total: data.total_seconds, items: data.items }))
      .catch(() => setDetail({ status: 'error' }))
  }

  async function confirmDelete() {
    if (deleting) return
    setDeleting(true)
    setFailed(false)
    try {
      await onDelete(group.group_id)
    } catch {
      setFailed(true)
      setDeleting(false)
    }
  }

  return (
    <li className="card p-6 transition duration-200 hover:border-ember-500/25 sm:p-7">
      {/* One row that balances for any number of goals: what the goal is on the left, its numbers and actions on the right. */}
      <div className="grid grid-cols-1 gap-6 md:grid-cols-[minmax(0,1fr)_auto] md:items-center">
        <div className="min-w-0">
          <h3 className="truncate text-2xl font-medium text-ink">{group.group_name}</h3>
          {group.description && <p className="mt-1 line-clamp-2 text-base leading-6 text-ink-mute">{group.description}</p>}

          <div
            className="mt-5 h-1 max-w-md rounded-full bg-night-700/60"
            role="progressbar"
            aria-label={`${group.group_name}: ${status}`}
            aria-valuemin={0}
            aria-valuemax={100}
            aria-valuenow={Math.round(progress * 100)}
          >
            <div
              className="h-full rounded-full bg-gradient-to-r from-ember-600 to-ember-400 transition-[width] duration-500"
              style={{ width: `${progress * 100}%` }}
            />
          </div>
          <p
            className={`mt-2.5 flex items-center gap-2.5 text-[15px] ${
              state === 'waiting' ? 'text-ember-300' : state === 'ready' ? 'text-ink-soft' : 'text-ink-mute'
            }`}
          >
            {/* The glow is kept for something that needs the user; "ready" is a calm tick, "not ready" a plain dot */}
            {state === 'ready' ? (
              <Check size={15} strokeWidth={2.5} aria-hidden="true" className="shrink-0 text-moss" />
            ) : (
              <span
                aria-hidden="true"
                className={`h-2 w-2 shrink-0 rounded-full ${
                  state === 'waiting' ? 'bg-ember-400 shadow-[0_0_8px_rgba(245,169,71,0.6)]' : 'bg-white/[0.18]'
                }`}
              />
            )}
            {status}
          </p>
        </div>

        <div className="flex flex-col gap-4 md:items-end">
          <dl className="flex gap-3">
            <div className="min-w-[7.5rem] rounded-card border border-white/[0.07] bg-night-900/50 px-5 py-3">
              <dt className="text-xs uppercase tracking-wider text-ink-faint">items</dt>
              <dd className="mt-1 font-display text-3xl text-ink" style={{ fontVariationSettings: '"opsz" 36' }}>
                {group.member_count}
              </dd>
            </div>
            <div className="min-w-[7.5rem] rounded-card border border-white/[0.07] bg-night-900/50 px-5 py-3">
              <dt className="text-xs uppercase tracking-wider text-ink-faint">time spent</dt>
              <dd className="mt-1 font-display text-3xl text-ink" style={{ fontVariationSettings: '"opsz" 36' }}>
                {seconds === 0 ? '0m' : spentText(seconds)}
              </dd>
            </div>
          </dl>

          {confirming ? (
            <div role="alert" className="max-w-sm md:text-right">
              <p className="text-[15px] leading-6 text-ink-soft">
                Delete “{group.group_name}”? Its rules stop running. Your items are not deleted.
              </p>
              {failed && <p className="mt-1.5 text-[15px] text-youtube">Couldn’t delete it. Try again.</p>}
              <div className="mt-3 flex gap-3 md:justify-end">
                <button
                  type="button"
                  onClick={() => void confirmDelete()}
                  disabled={deleting}
                  className="btn-ghost border-youtube/40 px-4 py-2 text-sm normal-case text-youtube hover:border-youtube/70 disabled:opacity-50"
                >
                  {deleting ? 'Deleting…' : 'Yes, delete'}
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setConfirming(false)
                    setFailed(false)
                  }}
                  disabled={deleting}
                  className="btn-ghost px-4 py-2 text-sm normal-case disabled:opacity-50"
                >
                  Keep it
                </button>
              </div>
            </div>
          ) : (
            <div className="flex flex-wrap items-center gap-3">
              <Link to={`/groups/${group.group_id}/items`} className={CARD_BUTTON} aria-label={`Items in ${group.group_name}`}>
                <ListChecks size={15} aria-hidden="true" /> Items
              </Link>
              <Link to={`/groups/${group.group_id}/rules`} className={CARD_BUTTON} aria-label={`Rules for ${group.group_name}`}>
                <SlidersHorizontal size={15} aria-hidden="true" /> Rules
              </Link>
              <button
                type="button"
                onClick={() => setConfirming(true)}
                className="inline-flex items-center gap-1.5 rounded-pill px-3 py-2 text-sm text-ink-faint transition hover:text-youtube"
                aria-label={`Delete ${group.group_name}`}
              >
                <Trash2 size={15} aria-hidden="true" /> Delete
              </button>
            </div>
          )}
        </div>
      </div>

      <button
        type="button"
        onClick={toggleContents}
        aria-expanded={open}
        aria-controls={open ? `group-contents-${group.group_id}` : undefined}
        className="group mt-6 flex w-full items-center justify-between gap-4 rounded-card border border-white/[0.07] bg-night-900/50 px-5 py-3.5 text-left transition hover:border-ember-500/30"
      >
        <span className="flex items-center gap-3 whitespace-nowrap text-base text-ink-soft transition group-hover:text-ink">
          <ChevronDown size={18} aria-hidden="true" className={`shrink-0 text-ember-400 transition-transform duration-200 ${open ? 'rotate-0' : '-rotate-90'}`} />
          What’s inside
        </span>
        <span className="hidden text-[15px] text-ink-mute sm:inline">{open ? 'Hide the items' : 'See the items and the time on each'}</span>
      </button>
      {open && detail && <GroupContents id={`group-contents-${group.group_id}`} detail={detail} />}
    </li>
  )
}

/** The goals: one full-width card each, or what to do when there are none yet. `groups` is null while it loads. */
function GroupGrid({
  groups,
  times,
  queue,
  onDelete,
}: {
  groups: UserGroup[] | null
  times: Record<string, number> | null
  queue: GroupSuggestion[] | null
  onDelete: (groupId: string) => Promise<void>
}) {
  if (groups === null) {
    return (
      <div className="grid grid-cols-1 gap-5 md:grid-cols-2" role="status" aria-label="Loading your goal groups">
        {[0, 1].map((index) => (
          <div key={index} className="h-44 animate-pulse rounded-card bg-night-850/60" />
        ))}
      </div>
    )
  }

  if (groups.length === 0) {
    return (
      <div className="card flex flex-col items-start gap-5 p-8">
        <span className="grid h-12 w-12 place-items-center rounded-full border border-ember-500/30 bg-ember-500/10 text-ember-400">
          <Target size={22} strokeWidth={1.75} aria-hidden="true" />
        </span>
        <div>
          <p className="font-display text-2xl italic text-ink-soft" style={{ fontVariationSettings: '"opsz" 28' }}>
            No goal groups yet.
          </p>
          <p className="mt-2 max-w-[52ch] text-base leading-7 text-ink-mute">
            A goal group collects the pages, emails and videos behind one goal, like “Placement Prep”. You pick{' '}
            {AUTO_SORT_AFTER} examples and Echo suggests the rest.
          </p>
        </div>
        <Link to="/groups/new" className={NEW_GROUP_CLASS}>
          <Plus size={17} aria-hidden="true" /> New goal group
        </Link>
      </div>
    )
  }

  return (
    <ul className="space-y-5">
      {groups.map((group) => (
        <GroupCard
          key={group.group_id}
          group={group}
          seconds={times === null ? undefined : (times[group.group_id] ?? 0)}
          waiting={(queue ?? []).filter((suggestion) => suggestion.group_id === group.group_id).length}
          onDelete={onDelete}
        />
      ))}
    </ul>
  )
}

/* ── Sunday review: what Echo added or suggests, waiting for the user ───── */
function ReviewRow({
  suggestion,
  onDecide,
}: {
  suggestion: GroupSuggestion
  onDecide: (suggestionId: string, accept: boolean) => void
}) {
  const automatic = suggestion.decision === 'auto_accepted'
  return (
    <motion.li
      layout
      exit={{ opacity: 0, x: 24, transition: { duration: 0.25 } }}
      className="flex flex-wrap items-center justify-between gap-4 border-b border-white/[0.05] py-4 last:border-0"
    >
      <div className="min-w-0 flex-1">
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5">
          <SourceBadge source={suggestion.source_type} />
          <span className="text-[15px] text-ink-faint">→ {suggestion.group_name}</span>
          {automatic && (
            <span className="rounded-pill border border-ember-500/30 px-2.5 py-0.5 font-mono text-xs text-ember-300">
              added automatically
            </span>
          )}
        </div>
        <p className="mt-1.5 truncate text-base text-ink-soft">{suggestion.title || '(untitled)'}</p>
      </div>
      <div className="flex shrink-0 gap-2.5">
        <button
          type="button"
          onClick={() => onDecide(suggestion.suggestion_id, true)}
          className="btn-ghost inline-flex items-center gap-2 px-4 py-2 text-sm"
          aria-label={`${automatic ? 'Keep in' : 'Add to'} ${suggestion.group_name}: ${suggestion.title || 'untitled item'}`}
        >
          <Check size={15} aria-hidden="true" className="text-moss" /> {automatic ? 'Keep' : 'Add'}
        </button>
        <button
          type="button"
          onClick={() => onDecide(suggestion.suggestion_id, false)}
          className="btn-ghost inline-flex items-center gap-2 px-4 py-2 text-sm"
          aria-label={`${automatic ? 'Remove from' : 'Do not add to'} ${suggestion.group_name}: ${suggestion.title || 'untitled item'}`}
        >
          <X size={15} aria-hidden="true" className="text-youtube" /> {automatic ? 'Remove' : 'Skip'}
        </button>
      </div>
    </motion.li>
  )
}

/* ── Page ───────────────────────────────────────────────────────────────── */
interface CreatedGroupNotice {
  name: string
  items: number
  hints: number
}

/**
 * Goal groups: the groups themselves lead the page, as cards. The Sunday review, what Echo added or
 * suggests, sits below them: a short strip when nothing waits, the list when something does.
 */
export default function GroupsPage() {
  // The new-goal flow sends the user back here with what it created. Show it, then blank the state in
  // the browser's history entry (not through the router, which would remount the page and lose the
  // notice) so a refresh does not bring it back.
  const location = useLocation()
  const [notice, setNotice] = useState<CreatedGroupNotice | null>(
    (location.state as { createdGroup?: CreatedGroupNotice } | null)?.createdGroup ?? null,
  )
  useEffect(() => {
    if (location.state) window.history.replaceState({ ...window.history.state, usr: null }, '')
  }, [location.state])

  // null while loading, so the page shows placeholders and not a "no groups yet" flash.
  const [groups, setGroups] = useState<UserGroup[] | null>(null)
  const [reviewQueue, setReviewQueue] = useState<GroupSuggestion[] | null>(null)
  // Seconds spent per group (all time); a second request, so the cards do not wait for it.
  const [times, setTimes] = useState<Record<string, number> | null>(null)
  // "Check now" in the review section: nothing runs the check by itself, so this starts it.
  const [checking, setChecking] = useState(false)
  const [checkResult, setCheckResult] = useState<AutoAssignResult | null>(null)
  const [checkFailed, setCheckFailed] = useState(false)
  const [explaining, setExplaining] = useState(false)

  useEffect(() => {
    let cancelled = false
    void fetchGroups().then((data) => !cancelled && setGroups(data))
    void fetchReviewQueue().then((data) => !cancelled && setReviewQueue(data))
    void fetchGroupTimes().then((data) => !cancelled && setTimes(data))
    return () => {
      cancelled = true
    }
  }, [])

  function handleDecide(suggestionId: string, accept: boolean) {
    setReviewQueue((current) => (current ?? []).filter((s) => s.suggestion_id !== suggestionId))
    void decideSuggestion(suggestionId, accept).then(() => {
      // Member counts may have changed — refresh the group list quietly.
      void fetchGroups().then(setGroups)
    })
  }

  // Only a group with enough confirmed items (the backend's auto_assignment_active) is checked against.
  const readyGroupCount = (groups ?? []).filter((group) => group.auto_assignment_active).length

  async function handleCheckNow() {
    setChecking(true)
    setCheckFailed(false)
    setCheckResult(null)
    try {
      const result = await runAutoAssignment()
      setCheckResult(result)
      // New suggestions and automatic additions land in the queue; member counts may have changed.
      void refreshReviewQueue()
        .then(setReviewQueue)
        .catch(() => undefined) // keep the queue already shown
      void fetchGroups().then(setGroups)
    } catch {
      setCheckFailed(true)
    } finally {
      setChecking(false)
    }
  }

  function checkSummary(result: AutoAssignResult): string {
    if (result.skipped) return 'A check is already running. Give it a moment.'
    if (result.auto_accepted === 0 && result.suggested === 0) return 'Nothing new matched.'
    const parts = []
    if (result.auto_accepted > 0) parts.push(`${result.auto_accepted} added automatically`)
    if (result.suggested > 0) parts.push(`${result.suggested} waiting for your yes`)
    return `Checked your recent items: ${parts.join(', ')}.`
  }

  // Throws on a refused delete, so the card can say so and stay in the grid.
  async function handleDeleteGroup(groupId: string) {
    await deleteGroup(groupId)
    setGroups((current) => (current ?? []).filter((group) => group.group_id !== groupId))
    // The review queue only lists suggestions of active groups, so drop this group's now.
    setReviewQueue((current) => (current ?? []).filter((suggestion) => suggestion.group_id !== groupId))
  }

  const waiting = reviewQueue?.length ?? 0

  return (
    <div>
      <PageTitle
        eyebrow="groups"
        title="Your goals."
        aside={
          groups !== null && groups.length > 0 ? (
            <Link to="/groups/new" className={NEW_GROUP_CLASS}>
              <Plus size={17} aria-hidden="true" /> New goal group
            </Link>
          ) : undefined
        }
      />

      <motion.p
        className="mt-6 max-w-[62ch] text-[17px] leading-8 text-ink-mute"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.6, delay: 0.35 }}
      >
        A goal group gathers the pages, emails and videos behind one goal. Pick a few examples and Echo suggests
        similar items, and you decide what stays.
      </motion.p>

      {notice && (
        <motion.div
          role="status"
          className="mt-6 flex items-start justify-between gap-4 rounded-card border border-ember-500/30 bg-ember-500/10 px-6 py-5"
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4, ease: EASE_OUT }}
        >
          <div className="flex items-start gap-3.5">
            <Check size={20} aria-hidden="true" className="mt-1 shrink-0 text-ember-400" />
            <p className="text-base leading-7 text-ink-soft">
              <span className="font-medium text-ink">“{notice.name}” is ready</span> with {notice.items}{' '}
              {notice.items === 1 ? 'example' : 'examples'}
              {notice.hints > 0 ? ` and ${notice.hints} ${notice.hints === 1 ? 'hint' : 'hints'}` : ''}. Echo will start
              suggesting similar items in your Sunday review.
            </p>
          </div>
          <button
            type="button"
            onClick={() => setNotice(null)}
            aria-label="Dismiss"
            className="shrink-0 rounded-full p-1.5 text-ink-faint transition hover:text-ink-soft"
          >
            <X size={16} aria-hidden="true" />
          </button>
        </motion.div>
      )}

      <section className="mt-10" aria-labelledby="groups-heading">
        <h2 id="groups-heading" className="mb-5 flex items-baseline gap-3 font-display text-3xl italic text-ink" style={{ fontVariationSettings: '"opsz" 36' }}>
          Your goal groups
          {groups && groups.length > 0 && <span className="font-mono text-base not-italic text-ink-faint">{groups.length}</span>}
        </h2>
        <GroupGrid groups={groups} times={times} queue={reviewQueue} onDelete={handleDeleteGroup} />
      </section>

      {/* The review: secondary to the goals. A strip when nothing waits, the list when something does. */}
      <section
        className={`card mt-12 p-6 sm:p-7 ${waiting > 0 ? 'border-ember-500/40 shadow-[0_0_50px_rgba(245,169,71,0.07)]' : ''}`}
        aria-labelledby="review-heading"
      >
        <div className="flex flex-wrap items-center justify-between gap-x-6 gap-y-4">
          <div>
            <h2 id="review-heading" className="font-display text-2xl italic text-ink" style={{ fontVariationSettings: '"opsz" 28' }}>
              The Sunday review
            </h2>
            <p className={`mt-1 text-base ${waiting > 0 ? 'text-ember-300' : 'text-ink-mute'}`}>
              {reviewQueue === null
                ? 'Loading…'
                : waiting > 0
                  ? `${waiting} ${waiting === 1 ? 'item is' : 'items are'} waiting for you.`
                  : 'Nothing to review. The queue is clear.'}
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-4">
            {groups !== null && readyGroupCount === 0 && (
              <span className="max-w-[30ch] text-[15px] leading-6 text-ink-faint">
                Echo starts checking once a group has {AUTO_SORT_AFTER} items you confirmed.
              </span>
            )}
            <button
              type="button"
              onClick={() => void handleCheckNow()}
              disabled={checking || readyGroupCount === 0}
              className="btn-ghost inline-flex items-center gap-2.5 px-5 py-2.5 text-base disabled:opacity-50"
            >
              <RefreshCw size={16} aria-hidden="true" className={checking ? 'motion-safe:animate-spin' : ''} />
              {checking ? 'Checking…' : 'Check recent items now'}
            </button>
          </div>
        </div>

        {checkResult && groups !== null && readyGroupCount > 0 && (
          <p role="status" className="mt-4 text-base text-ink-soft">
            {checkSummary(checkResult)}
          </p>
        )}
        {checkFailed && (
          <p role="alert" className="mt-4 text-base text-ink-soft">
            Couldn’t run the check just now. Try again in a moment.
          </p>
        )}

        {reviewQueue === null ? (
          <div className="mt-5 space-y-3" role="status" aria-label="Loading the review queue">
            {[0, 1].map((index) => (
              <div key={index} className="h-14 animate-pulse rounded bg-night-800/60" />
            ))}
          </div>
        ) : (
          waiting > 0 && (
            <ul className="mt-4">
              <AnimatePresence initial={false}>
                {reviewQueue.map((suggestion) => (
                  <ReviewRow key={suggestion.suggestion_id} suggestion={suggestion} onDecide={handleDecide} />
                ))}
              </AnimatePresence>
            </ul>
          )
        )}

        <div className="mt-5 border-t border-white/[0.06] pt-4">
          <button
            type="button"
            onClick={() => setExplaining((current) => !current)}
            aria-expanded={explaining}
            className="inline-flex items-center gap-2 text-[15px] text-ink-mute transition hover:text-ink-soft"
          >
            <ChevronDown size={16} aria-hidden="true" className={`text-ember-400 transition-transform duration-200 ${explaining ? 'rotate-0' : '-rotate-90'}`} />
            How does the review work?
          </button>
          {explaining && (
            <p className="mt-3 max-w-[70ch] text-[15px] leading-7 text-ink-mute">
              Echo never decides alone when it is unsure. If a rule you wrote and the similarity to items you confirmed
              both agree, the item is added and shown here, where one click undoes it. Anything less certain waits for
              your yes. Only items you confirmed ever teach Echo.
            </p>
          )}
        </div>
      </section>
    </div>
  )
}
