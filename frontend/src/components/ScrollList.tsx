import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react'
import { ChevronDown } from 'lucide-react'

interface Edges {
  top: boolean
  bottom: boolean
}

// A few pixels of slack so a list resting at its end never shows a stray hint.
const EDGE_SLACK_PX = 4

/**
 * A list that scrolls inside its card and says so: an amber scrollbar, a fade at every edge
 * that has more behind it, and a "scroll for more" chip (it scrolls one page when pressed)
 * that goes away at the end of the list. Pass `<li>` children.
 */
export default function ScrollList({ label, children }: { label: string; children: ReactNode }) {
  const listRef = useRef<HTMLUListElement>(null)
  const [edges, setEdges] = useState<Edges>({ top: false, bottom: false })

  const measure = useCallback(() => {
    const list = listRef.current
    if (!list) return
    const top = list.scrollTop > EDGE_SLACK_PX
    const bottom = list.scrollHeight - list.clientHeight - list.scrollTop > EDGE_SLACK_PX
    // Same values: return the old object so React skips the render.
    setEdges((current) => (current.top === top && current.bottom === bottom ? current : { top, bottom }))
  }, [])

  // The children change with every filter and add; the window changes with every resize.
  useEffect(measure)
  useEffect(() => {
    window.addEventListener('resize', measure)
    return () => window.removeEventListener('resize', measure)
  }, [measure])

  function scrollOnePage() {
    const list = listRef.current
    if (list) list.scrollBy({ top: list.clientHeight * 0.8, behavior: 'smooth' })
  }

  return (
    <div className="relative mt-3">
      <ul
        ref={listRef}
        onScroll={measure}
        tabIndex={0}
        aria-label={label}
        className="scroll-visible max-h-[60vh] overflow-y-auto rounded-sm pr-3 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ember-500/40"
      >
        {children}
      </ul>

      <div
        aria-hidden="true"
        className={`pointer-events-none absolute inset-x-0 top-0 h-10 bg-gradient-to-b from-night-850 to-transparent transition-opacity duration-200 ${
          edges.top ? 'opacity-100' : 'opacity-0'
        }`}
      />
      <div
        aria-hidden="true"
        className={`pointer-events-none absolute inset-x-0 bottom-0 h-16 bg-gradient-to-t from-night-850 via-night-850/70 to-transparent transition-opacity duration-200 ${
          edges.bottom ? 'opacity-100' : 'opacity-0'
        }`}
      />
      <button
        type="button"
        onClick={scrollOnePage}
        tabIndex={edges.bottom ? 0 : -1}
        aria-hidden={!edges.bottom}
        className={`absolute bottom-2 left-1/2 inline-flex -translate-x-1/2 items-center gap-1.5 rounded-pill border border-ember-500/40 bg-night-900 px-3.5 py-1.5 font-mono text-[11px] text-ember-400 shadow-lamp transition duration-200 hover:border-ember-400/70 hover:text-ember-300 ${
          edges.bottom ? 'opacity-100' : 'pointer-events-none opacity-0'
        }`}
      >
        <ChevronDown size={13} aria-hidden="true" /> scroll for more
      </button>
    </div>
  )
}
