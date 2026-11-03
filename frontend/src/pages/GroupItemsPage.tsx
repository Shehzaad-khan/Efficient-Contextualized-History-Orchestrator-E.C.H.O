import { useEffect, useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { motion } from 'framer-motion'
import { ArrowLeft, Check, Plus, Search } from 'lucide-react'
import PageTitle from '../components/PageTitle'
import SourceBadge from '../components/SourceBadge'
import { ApiError, assignToGroup, fetchGroupItems, fetchGroups, fetchRecent } from '../lib/api'
import { asUtc, formatWhen } from '../lib/format'
import type { GroupMember, MemoryItem, UserGroup } from '../lib/types'

// The most the backend's recent-items route gives (backend/wellbeing.py `/recent`, le=100).
const PICKER_ITEMS = 100
// Confirmed members needed before Echo starts suggesting items by similarity (A42.3).
const CONFIRMED_FOR_AUTO_SORT = 5

function itemLocation(item: MemoryItem): string {
  if (item.source_type === 'gmail') return item.sender ?? ''
  if (item.source_type === 'chrome') return item.domain ?? ''
  return item.channel_name ?? ''
}

/** True when every word typed appears in the item's title or its sender / site / channel. */
function matches(item: MemoryItem, words: string[]): boolean {
  const haystack = `${item.title ?? ''} ${itemLocation(item)}`.toLowerCase()
  return words.every((word) => haystack.includes(word))
}

function assignErrorText(error: unknown): string {
  if (error instanceof ApiError && error.status === 404) return 'That item or this group no longer exists.'
  return 'Couldn’t add it. Try again in a moment.'
}

/**
 * One goal group's members, and a picker to put items in it by hand (D6.4).
 *
 * Picking an item is his own approval: the backend writes it through
 * `manual_assign` (an accepted suggestion and the membership in one step), the one
 * write path into the group. Hand-picked items are the confirmed examples Echo
 * learns from; after five of them it starts suggesting similar items itself.
 */
export default function GroupItemsPage() {
  const { groupId = '' } = useParams()
  const [group, setGroup] = useState<UserGroup | null>(null)
  const [members, setMembers] = useState<GroupMember[] | null>(null)
  const [recent, setRecent] = useState<MemoryItem[] | null>(null)
  const [search, setSearch] = useState('')
  const [savingId, setSavingId] = useState<string | null>(null)
  const [failure, setFailure] = useState<{ memoryId: string; text: string } | null>(null)

  useEffect(() => {
    let cancelled = false
    setMembers(null)
    setRecent(null)
    void fetchGroups().then((groups) => !cancelled && setGroup(groups.find((g) => g.group_id === groupId) ?? null))
    void fetchGroupItems(groupId).then((data) => !cancelled && setMembers(data))
    void fetchRecent(PICKER_ITEMS).then((data) => !cancelled && setRecent(data))
    return () => {
      cancelled = true
    }
  }, [groupId])

  const memberIds = useMemo(() => new Set((members ?? []).map((member) => member.memory_id)), [members])

  const candidates = useMemo(() => {
    const words = search.toLowerCase().split(/\s+/).filter(Boolean)
    return (recent ?? []).filter((item) => matches(item, words))
  }, [recent, search])

  async function add(item: MemoryItem) {
    if (savingId) return
    setSavingId(item.memory_id)
    setFailure(null)
    try {
      const result = await assignToGroup(groupId, item.memory_id)
      setMembers((current) => [
        {
          memory_id: item.memory_id,
          source_type: item.source_type,
          title: item.title,
          created_at: item.created_at,
          assigned_at: new Date().toISOString(),
        },
        ...(current ?? []).filter((member) => member.memory_id !== item.memory_id),
      ])
      setGroup((current) =>
        current
          ? {
              ...current,
              member_count: result.member_count || current.member_count + 1,
              confirmed_count: result.confirmed_count || (current.confirmed_count ?? 0) + 1,
              auto_assignment_active: result.auto_assignment_active || current.auto_assignment_active,
            }
          : current,
      )
    } catch (error) {
      setFailure({ memoryId: item.memory_id, text: assignErrorText(error) })
    } finally {
      setSavingId(null)
    }
  }

  const confirmed = group?.confirmed_count ?? group?.member_count ?? 0
  const toAutoSort = Math.max(0, CONFIRMED_FOR_AUTO_SORT - confirmed)

  return (
    <div>
      <Link
        to="/patterns"
        className="meta mb-6 inline-flex items-center gap-1.5 text-ink-mute transition hover:text-ink-soft"
      >
        <ArrowLeft size={12} aria-hidden="true" /> patterns
      </Link>

      <PageTitle
        eyebrow={group ? `group items · ${group.group_name}` : 'group items'}
        title="What belongs here."
        aside={
          <div className="flex items-center gap-4">
            {members && <p className="meta">{members.length} in this group</p>}
            <Link to={`/groups/${groupId}/rules`} className="meta text-ember-400 transition hover:text-ember-300">
              rules →
            </Link>
          </div>
        }
      />

      <motion.p
        className="mt-6 max-w-[58ch] text-[15px] leading-7 text-ink-mute"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.6, delay: 0.35 }}
      >
        Pick the items that belong to this goal. Each one you add is an example Echo learns from
        {group?.auto_assignment_active
          ? '; it already suggests similar items in the Sunday review.'
          : toAutoSort > 0
            ? `; after ${toAutoSort} more it starts suggesting similar ones in the Sunday review.`
            : '.'}
      </motion.p>

      <div className="mt-10 grid max-w-5xl grid-cols-1 gap-8 lg:grid-cols-2">
        <section className="card px-6 py-5" aria-labelledby="picker-heading">
          <h2 id="picker-heading" className="meta">
            add from your recent items
          </h2>
          <label className="input-study mt-4 flex items-center gap-2 px-3 py-2">
            <Search size={13} aria-hidden="true" className="shrink-0 text-ink-faint" />
            <input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="filter by title, sender, site or channel…"
              aria-label="Filter recent items"
              className="flex-1 bg-transparent text-sm text-ink placeholder:text-ink-faint focus:outline-none"
            />
          </label>

          {recent === null ? (
            <div className="mt-4 space-y-3" role="status" aria-label="Loading recent items">
              {[0, 1, 2, 3].map((index) => (
                <div key={index} className="h-12 animate-pulse rounded bg-night-800/60" />
              ))}
            </div>
          ) : candidates.length === 0 ? (
            <p className="mt-4 text-sm text-ink-mute">
              {recent.length === 0 ? 'No items yet.' : 'Nothing in your recent items matches that.'}
            </p>
          ) : (
            <ul className="mt-3 max-h-[60vh] overflow-y-auto pr-1">
              {candidates.map((item) => {
                const inGroup = memberIds.has(item.memory_id)
                const saving = savingId === item.memory_id
                return (
                  <li
                    key={item.memory_id}
                    className="flex items-center justify-between gap-4 border-b border-white/[0.05] py-3 last:border-0"
                  >
                    <div className="min-w-0">
                      <div className="flex flex-wrap items-center gap-2">
                        <SourceBadge source={item.source_type} />
                        <span className="meta text-ink-faint">{formatWhen(asUtc(item.last_accessed_at ?? item.created_at))}</span>
                      </div>
                      <p className="mt-1 truncate text-sm text-ink-soft">{item.title || '(untitled)'}</p>
                      {itemLocation(item) && <p className="truncate text-xs text-ink-mute">{itemLocation(item)}</p>}
                      {failure?.memoryId === item.memory_id && (
                        <p role="alert" className="mt-1 text-xs text-youtube">
                          {failure.text}
                        </p>
                      )}
                    </div>
                    {inGroup ? (
                      <span className="meta inline-flex shrink-0 items-center gap-1 text-moss">
                        <Check size={12} aria-hidden="true" /> in group
                      </span>
                    ) : (
                      <button
                        type="button"
                        onClick={() => void add(item)}
                        disabled={savingId !== null}
                        className="btn-ghost inline-flex shrink-0 items-center gap-1.5 text-xs disabled:opacity-40"
                        aria-label={`Add to ${group?.group_name ?? 'group'}: ${item.title || 'untitled item'}`}
                      >
                        <Plus size={13} aria-hidden="true" /> {saving ? 'adding…' : 'add'}
                      </button>
                    )}
                  </li>
                )
              })}
            </ul>
          )}
          <p className="meta mt-3 text-[9px] text-ink-faint">your {PICKER_ITEMS} most recent items · search Recall for older ones</p>
        </section>

        <section className="card px-6 py-5" aria-labelledby="members-heading">
          <h2 id="members-heading" className="meta">
            in this group{members ? ` · ${members.length}` : ''}
          </h2>
          {members === null ? (
            <div className="mt-4 space-y-3" role="status" aria-label="Loading group items">
              {[0, 1, 2].map((index) => (
                <div key={index} className="h-10 animate-pulse rounded bg-night-800/60" />
              ))}
            </div>
          ) : members.length === 0 ? (
            <p className="mt-4 text-sm text-ink-mute">Nothing here yet. Add items from the list.</p>
          ) : (
            <ul className="mt-3 max-h-[60vh] overflow-y-auto pr-1">
              {members.map((member) => (
                <li key={member.memory_id} className="border-b border-white/[0.05] py-3 last:border-0">
                  <div className="flex flex-wrap items-center gap-2">
                    <SourceBadge source={member.source_type} />
                    <span className="meta text-ink-faint">added {formatWhen(asUtc(member.assigned_at))}</span>
                  </div>
                  <p className="mt-1 truncate text-sm text-ink-soft">{member.title || '(untitled)'}</p>
                </li>
              ))}
            </ul>
          )}
        </section>
      </div>
    </div>
  )
}
