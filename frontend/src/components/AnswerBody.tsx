import { Fragment, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import { Globe, Mail, Play } from 'lucide-react'
import type { AnswerSource, SourceType } from '../lib/types'

/**
 * An Echo answer drawn as chat text with memory pills (A27).
 *
 * `markdown` is the reply with ⟦n⟧ markers; each marker becomes a pill for
 * `sources[n]`. A pill names the human anchor (sender, site, channel) and the
 * date, opens the original in a new tab, and shows a peek card on hover or
 * keyboard focus. Below the text, a moment strip places the cited memories on a
 * time ribbon; pointing at a pill lights its dot and the other way round. The
 * strip is hidden when every cited memory is from the same day.
 *
 * Only React elements are built (no HTML injection): the text comes from an
 * LLM, which may echo text from a web page or an email.
 */

const SOURCE_ICON: Record<SourceType, typeof Mail> = { gmail: Mail, chrome: Globe, youtube: Play }
const SOURCE_NAME: Record<SourceType, string> = { gmail: 'Email', chrome: 'Page', youtube: 'Video' }
const SOURCE_DOT: Record<SourceType, string> = { gmail: 'bg-gmail', chrome: 'bg-chrome', youtube: 'bg-youtube' }

const INLINE_TOKEN = /(\*\*[^*\n]+\*\*|⟦\d{1,2}⟧)/g
const LIST_LINE = /^\s*(?:[-*]|\d+\.)\s+(.*)$/
const MARKER = /^⟦(\d{1,2})⟧$/

interface Block {
  kind: 'p' | 'list'
  lines: string[]
}

/** Paragraphs and bullet / numbered lists; a blank line ends a block. */
function toBlocks(markdown: string): Block[] {
  const blocks: Block[] = []
  for (const line of markdown.split('\n')) {
    if (!line.trim()) {
      blocks.push({ kind: 'p', lines: [] }) // separator; empty blocks are dropped below
      continue
    }
    const kind = LIST_LINE.test(line) ? 'list' : 'p'
    const last = blocks[blocks.length - 1]
    if (last && last.lines.length > 0 && last.kind === kind) last.lines.push(line)
    else blocks.push({ kind, lines: [line] })
  }
  return blocks.filter((block) => block.lines.length > 0)
}

function sourceLabel(source: AnswerSource): string {
  const kind = source.source_type ? SOURCE_NAME[source.source_type] : 'Memory'
  const when = source.date_label ? `, ${source.date_label}` : ''
  return `${kind} from ${source.anchor}${when}`
}

function exactTime(iso: string | null): string {
  if (!iso) return 'unknown time'
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return 'unknown time'
  return date.toLocaleString(undefined, {
    weekday: 'short',
    day: 'numeric',
    month: 'short',
    year: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  })
}

function MemoryPill({
  source,
  active,
  onActive,
}: {
  source: AnswerSource
  active: boolean
  onActive: (n: number | null) => void
}) {
  const Icon = source.source_type ? SOURCE_ICON[source.source_type] : Globe
  const related = source.role === 'related'
  const tooltipId = `peek-${source.n}-${source.memory_id ?? 'x'}`
  const className =
    `inline-flex items-center gap-1 rounded-pill border px-2 py-0.5 align-baseline font-mono text-[11.5px] leading-5 transition ` +
    `${related ? 'border-dashed border-white/20 text-ink-mute' : 'border-white/[0.12] bg-night-800 text-ink-soft'} ` +
    `${active ? 'border-ember-400/70 text-ink' : ''} hover:border-ember-400/60 hover:text-ink focus-visible:outline focus-visible:outline-1 focus-visible:outline-ember-400`
  const content = (
    <>
      <Icon size={11} aria-hidden="true" />
      <span>{source.anchor}</span>
      {source.date_label && <span className="text-ink-faint">· {source.date_label}</span>}
      {related && <span className="text-ink-faint">· related</span>}
    </>
  )
  return (
    <span
      className="group relative mx-0.5 inline-block"
      onMouseEnter={() => onActive(source.n)}
      onMouseLeave={() => onActive(null)}
      onFocus={() => onActive(source.n)}
      onBlur={() => onActive(null)}
    >
      {source.link ? (
        <a
          href={source.link}
          target="_blank"
          rel="noopener noreferrer"
          className={className}
          aria-label={`${sourceLabel(source)}: ${source.title}. Opens in a new tab.`}
          aria-describedby={tooltipId}
        >
          {content}
        </a>
      ) : (
        <span className={className} tabIndex={0} aria-label={`${sourceLabel(source)}: ${source.title}`} aria-describedby={tooltipId}>
          {content}
        </span>
      )}
      <span
        id={tooltipId}
        role="tooltip"
        className="pointer-events-none invisible absolute left-0 top-full z-20 mt-2 w-64 rounded-card border border-white/[0.08] bg-night-900 p-3 text-left opacity-0 shadow-raise transition group-focus-within:visible group-focus-within:opacity-100 group-hover:visible group-hover:opacity-100 motion-reduce:transition-none"
      >
        <span className="meta block">{source.source_type ? SOURCE_NAME[source.source_type] : 'Memory'}</span>
        <span className="mt-1 line-clamp-2 block font-body text-[13px] leading-5 text-ink">{source.title}</span>
        <span className="mt-1.5 block font-mono text-[11px] text-ink-mute">{exactTime(source.created_at_local)}</span>
      </span>
    </span>
  )
}

/** Plain text, **bold** and ⟦n⟧ pills inside one line of the answer. */
function renderInline(
  text: string,
  sources: Map<number, AnswerSource>,
  activeN: number | null,
  onActive: (n: number | null) => void,
): ReactNode[] {
  return text.split(INLINE_TOKEN).map((part, index) => {
    if (part.startsWith('**') && part.endsWith('**') && part.length > 4) {
      return (
        <strong key={index} className="font-semibold text-ink">
          {part.slice(2, -2)}
        </strong>
      )
    }
    const marker = MARKER.exec(part)
    if (marker) {
      const source = sources.get(Number(marker[1]))
      return source ? <MemoryPill key={index} source={source} active={activeN === source.n} onActive={onActive} /> : null
    }
    return <Fragment key={index}>{part}</Fragment>
  })
}

function MomentStrip({
  sources,
  activeN,
  onActive,
}: {
  sources: AnswerSource[]
  activeN: number | null
  onActive: (n: number | null) => void
}) {
  const dated = sources
    .map((source) => ({ source, time: source.created_at_local ? new Date(source.created_at_local).getTime() : NaN }))
    .filter((entry) => !Number.isNaN(entry.time))
  const days = new Set(dated.map((entry) => new Date(entry.time).toDateString()))
  if (days.size < 2) return null

  const first = Math.min(...dated.map((entry) => entry.time))
  const last = Math.max(...dated.map((entry) => entry.time))
  const short = (time: number) => new Date(time).toLocaleDateString(undefined, { day: 'numeric', month: 'short' })

  return (
    <div className="mt-5 max-w-[68ch]" role="img" aria-label={`The cited memories span ${short(first)} to ${short(last)}`}>
      <div className="relative h-5">
        <span aria-hidden="true" className="absolute left-0 right-0 top-1/2 h-px bg-white/[0.12]" />
        {dated.map(({ source, time }) => {
          const percent = 1 + ((time - first) / (last - first)) * 98
          const type = source.source_type ?? 'chrome'
          return (
            <span
              key={source.n}
              aria-hidden="true"
              title={`${sourceLabel(source)}: ${source.title}`}
              onMouseEnter={() => onActive(source.n)}
              onMouseLeave={() => onActive(null)}
              style={{ left: `${percent}%` }}
              className={`absolute top-1/2 h-2.5 w-2.5 -translate-x-1/2 -translate-y-1/2 rounded-full transition ${SOURCE_DOT[type]} ${
                activeN === source.n ? 'scale-150 ring-2 ring-ember-400/70' : 'opacity-80'
              }`}
            />
          )
        })}
      </div>
      <div className="flex justify-between font-mono text-[10px] text-ink-faint" aria-hidden="true">
        <span>{short(first)}</span>
        <span>{short(last)}</span>
      </div>
    </div>
  )
}

export default function AnswerBody({ markdown, sources }: { markdown: string; sources: AnswerSource[] }) {
  const [activeN, setActiveN] = useState<number | null>(null)
  const byNumber = useMemo(() => new Map(sources.map((source) => [source.n, source])), [sources])
  const blocks = useMemo(() => toBlocks(markdown), [markdown])

  return (
    <div>
      <div className="max-w-[68ch] space-y-3 text-[15px] leading-7 text-ink-soft">
        {blocks.map((block, blockIndex) =>
          block.kind === 'list' ? (
            <ul key={blockIndex} className="list-disc space-y-1.5 pl-5 marker:text-ink-faint">
              {block.lines.map((line, lineIndex) => (
                <li key={lineIndex}>{renderInline(LIST_LINE.exec(line)?.[1] ?? line, byNumber, activeN, setActiveN)}</li>
              ))}
            </ul>
          ) : (
            <p key={blockIndex} className="whitespace-pre-line">
              {renderInline(block.lines.join('\n'), byNumber, activeN, setActiveN)}
            </p>
          ),
        )}
      </div>
      <MomentStrip sources={sources} activeN={activeN} onActive={setActiveN} />
    </div>
  )
}
