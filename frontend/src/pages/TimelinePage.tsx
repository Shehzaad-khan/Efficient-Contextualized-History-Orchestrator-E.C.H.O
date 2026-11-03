import { useEffect, useMemo, useRef, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { ArrowDown, ExternalLink, Flag, LoaderCircle, Trash2 } from 'lucide-react'
import gsap from 'gsap'
import { ScrollTrigger } from 'gsap/ScrollTrigger'
import PageTitle from '../components/PageTitle'
import SourceBadge from '../components/SourceBadge'
import { deleteItem, fetchRecentPage, toggleRegret } from '../lib/api'
import { asUtc, dayLabel, formatWhen } from '../lib/format'
import type { MemoryItem } from '../lib/types'

gsap.registerPlugin(ScrollTrigger)

const EASE_OUT = [0.22, 1, 0.36, 1] as const

function itemLocation(item: MemoryItem): string {
  if (item.source_type === 'gmail') return item.sender ?? 'unknown sender'
  if (item.source_type === 'chrome') return item.domain ?? 'unknown site'
  return item.channel_name ?? 'unknown channel'
}

/** The "open" button's words: where the link goes (D6.7: every source has one now). */
const OPEN_LABEL: Record<MemoryItem['source_type'], string> = {
  gmail: 'open in gmail',
  chrome: 'open page',
  youtube: 'open on youtube',
}

function TimelineEntry({
  item,
  index,
  onDeleted,
}: {
  item: MemoryItem
  index: number
  onDeleted: (memoryId: string) => void
}) {
  const [open, setOpen] = useState(false)
  const [marked, setMarked] = useState(false)
  const [note, setNote] = useState('')
  // Delete is permanent, so it asks first: "Delete" / "Keep", like deleting a past chat.
  const [confirming, setConfirming] = useState(false)
  const [deleting, setDeleting] = useState(false)
  const [deleteError, setDeleteError] = useState(false)

  async function markRegret() {
    try {
      await toggleRegret(item.memory_id, note.trim() || undefined)
      setMarked(true)
      setNote('')
    } catch {
      /* stays unmarked; nothing judgmental to say */
    }
  }

  async function confirmDelete() {
    setDeleting(true)
    setDeleteError(false)
    try {
      await deleteItem(item.memory_id)
      onDeleted(item.memory_id) // the entry leaves the list; this component unmounts
    } catch {
      setDeleteError(true)
      setDeleting(false)
    }
  }

  return (
    <motion.li
      initial={{ opacity: 0, x: -14 }}
      whileInView={{ opacity: 1, x: 0 }}
      viewport={{ once: true, margin: '-40px' }}
      transition={{ duration: 0.5, ease: EASE_OUT, delay: (index % 6) * 0.04 }}
      className="relative pl-10"
    >
      {/* Node on the thread */}
      <span
        aria-hidden="true"
        className={`absolute left-[13px] top-5 h-[7px] w-[7px] rounded-full transition ${
          item.source_type === 'gmail' ? 'bg-gmail' : item.source_type === 'youtube' ? 'bg-youtube' : 'bg-chrome'
        }`}
      />

      <div className={`card card-hover px-5 py-4 ${open ? 'border-ember-500/25' : ''}`}>
        <button
          type="button"
          onClick={() => setOpen((value) => !value)}
          aria-expanded={open}
          className="flex w-full items-start justify-between gap-4 text-left"
        >
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-3">
              <SourceBadge source={item.source_type} />
              <span className="meta text-ink-faint">{formatWhen(asUtc(item.last_accessed_at ?? item.created_at))}</span>
            </div>
            <h3 className="mt-2 truncate text-[15px] font-medium text-ink">{item.title || '(untitled)'}</h3>
          </div>
          <span className="meta mt-1 shrink-0 text-ink-faint">{open ? 'close' : 'detail'}</span>
        </button>

        <AnimatePresence initial={false}>
          {open && (
            <motion.div
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: 'auto', opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              transition={{ duration: 0.35, ease: EASE_OUT }}
              className="overflow-hidden"
            >
              <div className="mt-4 border-t border-white/[0.06] pt-4">
                <dl className="grid grid-cols-2 gap-x-6 gap-y-2 sm:grid-cols-3">
                  <div>
                    <dt className="meta">from</dt>
                    <dd className="mt-1 truncate text-sm text-ink-soft">{itemLocation(item)}</dd>
                  </div>
                  <div>
                    <dt className="meta">captured</dt>
                    <dd className="mt-1 text-sm text-ink-soft">{formatWhen(asUtc(item.created_at))}</dd>
                  </div>
                  {item.is_short && (
                    <div>
                      <dt className="meta">format</dt>
                      <dd className="mt-1 text-sm text-ink-soft">Short</dd>
                    </div>
                  )}
                </dl>

                <div className="mt-4 flex flex-wrap items-center gap-2">
                  {item.url && (
                    <a href={item.url} target="_blank" rel="noreferrer" className="btn-ghost inline-flex items-center gap-2 text-xs">
                      <ExternalLink size={13} aria-hidden="true" /> {OPEN_LABEL[item.source_type] ?? 'open source'}
                    </a>
                  )}
                  {marked ? (
                    <span className="meta text-ember-300">marked — your call, no judgment</span>
                  ) : (
                    <div className="flex flex-1 flex-wrap items-center gap-2">
                      <input
                        value={note}
                        onChange={(event) => setNote(event.target.value)}
                        placeholder="optional note to your future self…"
                        aria-label="Regret note"
                        className="input-study min-w-40 flex-1 px-3 py-1.5 text-xs"
                      />
                      <button type="button" onClick={() => void markRegret()} className="btn-ghost inline-flex items-center gap-2 text-xs">
                        <Flag size={13} aria-hidden="true" /> mark regret
                      </button>
                    </div>
                  )}
                  {!confirming && (
                    <button
                      type="button"
                      onClick={() => setConfirming(true)}
                      className="btn-ghost inline-flex items-center gap-2 text-xs"
                      aria-label={`Delete memory: ${item.title || 'untitled'}`}
                    >
                      <Trash2 size={13} aria-hidden="true" /> delete
                    </button>
                  )}
                </div>

                {confirming && (
                  <div className="mt-3 rounded-card bg-night-800 px-4 py-3" role="alertdialog" aria-label="Confirm delete">
                    <p className="text-xs leading-5 text-ink-soft">
                      Delete this memory for good? Its text, notes and search entry are removed everywhere, and this
                      cannot be undone. If you visit the page again, Echo can keep it as a new memory.
                    </p>
                    <div className="mt-2 flex gap-2">
                      <button
                        type="button"
                        onClick={() => void confirmDelete()}
                        disabled={deleting}
                        className="btn-ghost border-youtube/40 px-3 py-1 font-mono text-xs normal-case text-youtube disabled:opacity-50"
                      >
                        {deleting ? 'deleting…' : 'Delete'}
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          setConfirming(false)
                          setDeleteError(false)
                        }}
                        disabled={deleting}
                        className="btn-ghost px-3 py-1 font-mono text-xs normal-case disabled:opacity-50"
                      >
                        Keep
                      </button>
                    </div>
                    {deleteError && (
                      <p role="alert" className="mt-2 text-xs leading-5 text-ink-soft">
                        Couldn’t delete it just now — it’s still here. Try again in a moment.
                      </p>
                    )}
                  </div>
                )}
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </motion.li>
  )
}

const PAGE_SIZE = 40

/** `older` appended after `current`, leaving out items already shown (a day can span two pages). */
function appendUnique(current: MemoryItem[], older: MemoryItem[]): MemoryItem[] {
  const seen = new Set(current.map((item) => item.memory_id))
  return [...current, ...older.filter((item) => !seen.has(item.memory_id))]
}

export default function TimelinePage() {
  const [items, setItems] = useState<MemoryItem[] | null>(null)
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [loadingOlder, setLoadingOlder] = useState(false)
  const [olderFailed, setOlderFailed] = useState(false)
  const threadRef = useRef<HTMLDivElement>(null)
  const listRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    let cancelled = false
    void fetchRecentPage(PAGE_SIZE).then((page) => {
      if (cancelled) return
      setItems(page.items)
      setNextCursor(page.next_cursor)
    })
    return () => {
      cancelled = true
    }
  }, [])

  async function loadOlder() {
    if (nextCursor === null || loadingOlder) return
    setLoadingOlder(true)
    setOlderFailed(false)
    try {
      const page = await fetchRecentPage(PAGE_SIZE, nextCursor)
      setItems((current) => appendUnique(current ?? [], page.items))
      setNextCursor(page.next_cursor)
    } catch {
      setOlderFailed(true) // what is shown stays; the button below tries again
    } finally {
      setLoadingOlder(false)
    }
  }

  // GSAP + ScrollTrigger: the thread draws itself as you read down the page.
  // Runs again when older items are appended: the cleanup kills the old trigger, and the
  // new one measures the taller list.
  useEffect(() => {
    if (!items || !threadRef.current || !listRef.current) return
    if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return
    const trigger = gsap.fromTo(
      threadRef.current,
      { scaleY: 0 },
      {
        scaleY: 1,
        ease: 'none',
        scrollTrigger: { trigger: listRef.current, start: 'top 70%', end: 'bottom 60%', scrub: 0.6 },
      },
    )
    return () => {
      trigger.scrollTrigger?.kill()
      trigger.kill()
    }
  }, [items])

  function handleDeleted(memoryId: string) {
    setItems((current) => (current ? current.filter((entry) => entry.memory_id !== memoryId) : current))
  }

  const grouped = useMemo(() => {
    const groups = new Map<string, MemoryItem[]>()
    for (const item of items ?? []) {
      const key = dayLabel(asUtc(item.last_accessed_at ?? item.created_at))
      const list = groups.get(key) ?? []
      list.push(item)
      groups.set(key, list)
    }
    return [...groups.entries()]
  }, [items])

  return (
    <div>
      <PageTitle eyebrow="timeline" title="What Echo kept." />

      <div ref={listRef} className="relative mt-12">
        {/* The spine of time — grows with scroll */}
        <div
          ref={threadRef}
          aria-hidden="true"
          className="thread absolute bottom-0 left-4 top-0 origin-top"
        />

        {items === null && (
          <div className="space-y-4 pl-10" role="status" aria-label="Loading timeline">
            {[0, 1, 2, 3].map((index) => (
              <div key={index} className="card h-20 animate-pulse bg-night-850/60" />
            ))}
          </div>
        )}

        {items !== null && grouped.length === 0 && (
          <p className="pl-10 font-display text-lg italic text-ink-mute">
            Nothing here yet — Echo starts remembering as soon as the connectors run.
          </p>
        )}

        <div className="space-y-10">
          {grouped.map(([day, dayItems]) => (
            <section key={day} aria-label={day}>
              <h2 className="meta mb-4 pl-10 text-ember-400/90">{day}</h2>
              <ul className="space-y-3.5">
                {dayItems.map((item, index) => (
                  <TimelineEntry key={item.memory_id} item={item} index={index} onDeleted={handleDeleted} />
                ))}
              </ul>
            </section>
          ))}
        </div>
      </div>

      {/* Outside the list, so the thread ends at the last item */}
      {items !== null && items.length > 0 && (
        <div className="relative pb-4 pl-10">
          {/* The thread carries on, dotted and fading, down to the button */}
          <span
            aria-hidden="true"
            className="absolute left-4 top-0 h-8 border-l border-dashed border-ember-500/40"
          />
          <span
            aria-hidden="true"
            className="absolute left-[13px] top-8 h-[7px] w-[7px] rounded-full bg-ember-500/70"
          />
          {nextCursor !== null ? (
            <div className="flex flex-wrap items-center gap-4 pt-6">
              <button
                type="button"
                onClick={() => void loadOlder()}
                disabled={loadingOlder}
                className="btn-ghost group inline-flex items-center gap-2.5 border-ember-500/30 bg-night-850 px-5 py-2.5 text-ink shadow-raise hover:-translate-y-0.5 hover:border-ember-500/60 hover:shadow-[0_0_28px_-8px_rgba(245,169,71,0.45)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember-500/40 disabled:cursor-wait disabled:opacity-70 disabled:hover:translate-y-0 motion-reduce:transition-none motion-reduce:hover:translate-y-0"
              >
                {loadingOlder ? (
                  <LoaderCircle size={15} aria-hidden="true" className="text-ember-400 motion-safe:animate-spin" />
                ) : (
                  <ArrowDown
                    size={15}
                    aria-hidden="true"
                    className="text-ember-400 transition duration-300 motion-safe:group-hover:translate-y-0.5"
                  />
                )}
                <span>{loadingOlder ? 'loading…' : 'Load older'}</span>
              </button>
              {olderFailed && (
                <span role="status" className="meta text-ink-faint">
                  Couldn't load older items — press the button to try again.
                </span>
              )}
            </div>
          ) : (
            <p className="pt-6 font-display text-base italic text-ink-mute">That's everything Echo kept.</p>
          )}
        </div>
      )}
    </div>
  )
}
