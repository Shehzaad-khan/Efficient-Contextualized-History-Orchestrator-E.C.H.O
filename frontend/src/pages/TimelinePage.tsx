import { useEffect, useMemo, useRef, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { ExternalLink, Flag } from 'lucide-react'
import gsap from 'gsap'
import { ScrollTrigger } from 'gsap/ScrollTrigger'
import PageTitle from '../components/PageTitle'
import SourceBadge from '../components/SourceBadge'
import { fetchRecent, toggleRegret } from '../lib/api'
import { dayLabel, formatWhen } from '../lib/format'
import type { MemoryItem } from '../lib/types'

gsap.registerPlugin(ScrollTrigger)

const EASE_OUT = [0.22, 1, 0.36, 1] as const

function itemLocation(item: MemoryItem): string {
  if (item.source_type === 'gmail') return item.sender ?? 'unknown sender'
  if (item.source_type === 'chrome') return item.domain ?? 'unknown site'
  return item.channel_name ?? 'unknown channel'
}

function TimelineEntry({ item, index }: { item: MemoryItem; index: number }) {
  const [open, setOpen] = useState(false)
  const [marked, setMarked] = useState(false)
  const [note, setNote] = useState('')

  async function markRegret() {
    try {
      await toggleRegret(item.memory_id, note.trim() || undefined)
      setMarked(true)
      setNote('')
    } catch {
      /* stays unmarked; nothing judgmental to say */
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
              <span className="meta text-ink-faint">{formatWhen(item.last_accessed_at ?? item.created_at)}</span>
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
                    <dd className="mt-1 text-sm text-ink-soft">{formatWhen(item.created_at)}</dd>
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
                      <ExternalLink size={13} aria-hidden="true" /> open source
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
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </motion.li>
  )
}

export default function TimelinePage() {
  const [items, setItems] = useState<MemoryItem[] | null>(null)
  const threadRef = useRef<HTMLDivElement>(null)
  const listRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    let cancelled = false
    void fetchRecent(40).then((data) => {
      if (!cancelled) setItems(data)
    })
    return () => {
      cancelled = true
    }
  }, [])

  // GSAP + ScrollTrigger: the thread draws itself as you read down the page.
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

  const grouped = useMemo(() => {
    const groups = new Map<string, MemoryItem[]>()
    for (const item of items ?? []) {
      const key = dayLabel(item.last_accessed_at ?? item.created_at)
      const list = groups.get(key) ?? []
      list.push(item)
      groups.set(key, list)
    }
    return [...groups.entries()]
  }, [items])

  return (
    <div>
      <PageTitle
        eyebrow="timeline"
        title="What Echo kept."
        aside={items ? <p className="meta">{items.length} memories in view</p> : undefined}
      />

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
                  <TimelineEntry key={item.memory_id} item={item} index={index} />
                ))}
              </ul>
            </section>
          ))}
        </div>
      </div>
    </div>
  )
}
