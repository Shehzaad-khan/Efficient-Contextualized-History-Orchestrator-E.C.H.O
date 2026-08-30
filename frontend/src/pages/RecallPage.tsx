import { useCallback, useEffect, useRef, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { CornerDownLeft } from 'lucide-react'
import PageTitle from '../components/PageTitle'
import { askEcho } from '../lib/api'
import type { ChatMessage } from '../lib/types'

const EASE_OUT = [0.22, 1, 0.36, 1] as const

const SUGGESTIONS = [
  'What did I study after the interview email?',
  'Find the capstone review email',
  'Videos I watched about OS scheduling',
  'What was I reading last Tuesday night?',
]

let messageCounter = 0
const nextId = () => `m${++messageCounter}`

/** A pulsing dot on the thread while Echo searches. */
function ThinkingIndicator() {
  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="relative pl-7"
      role="status"
      aria-live="polite"
    >
      <span aria-hidden="true" className="thread absolute bottom-0 left-2 top-0" />
      <span aria-hidden="true" className="absolute left-[4.5px] top-2 h-2 w-2 animate-breathe rounded-full bg-ember-400" />
      <p className="font-mono text-xs uppercase tracking-[0.16em] text-ink-mute">
        searching your memory
        <span className="animate-breathe">…</span>
      </p>
      <div className="shimmer-line mt-3 h-px w-48" aria-hidden="true" />
    </motion.div>
  )
}

function EchoAnswer({ message }: { message: ChatMessage }) {
  return (
    <motion.article
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.55, ease: EASE_OUT }}
      className="relative pl-7"
    >
      {/* The thread — every answer hangs off the line of time */}
      <span aria-hidden="true" className="thread absolute bottom-0 left-2 top-1" />
      <span aria-hidden="true" className="absolute left-[4.5px] top-1.5 h-2 w-2 rounded-full bg-ember-400 shadow-thread" />

      <p className="meta mb-2">
        echo{typeof message.resultCount === 'number' && !message.noResults ? ` · ${message.resultCount} memories` : ''}
      </p>
      <div className="max-w-[68ch] whitespace-pre-wrap text-[15px] leading-7 text-ink-soft">
        {message.text}
      </div>
    </motion.article>
  )
}

function UserMessage({ text }: { text: string }) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.35, ease: EASE_OUT }}
      className="flex justify-end"
    >
      <p className="max-w-[52ch] rounded-card rounded-br-sm border border-white/[0.07] bg-night-800 px-4 py-3 text-[15px] leading-6 text-ink">
        {text}
      </p>
    </motion.div>
  )
}

export default function RecallPage() {
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const sessionRef = useRef<string | null>(null)
  const endRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' })
  }, [messages, busy])

  const send = useCallback(
    async (raw?: string) => {
      const query = (raw ?? input).trim()
      if (!query || busy) return
      setInput('')
      setBusy(true)
      setMessages((current) => [...current, { id: nextId(), role: 'user', text: query }])

      try {
        const reply = await askEcho(query, sessionRef.current)
        sessionRef.current = reply.session_id
        setMessages((current) => [
          ...current,
          {
            id: nextId(),
            role: 'echo',
            text: reply.final_answer,
            resultCount: reply.result_count,
            noResults: reply.no_results,
          },
        ])
      } catch {
        setMessages((current) => [
          ...current,
          { id: nextId(), role: 'echo', text: 'Something went wrong while reaching your memory. Try again in a moment.', noResults: true },
        ])
      } finally {
        setBusy(false)
        inputRef.current?.focus()
      }
    },
    [busy, input],
  )

  const empty = messages.length === 0

  return (
    <div className="flex min-h-[calc(100vh-6rem)] flex-col">
      <PageTitle
        eyebrow="recall"
        title="Ask your memory."
        aside={<p className="meta">gmail · chrome · youtube</p>}
      />

      {/* Conversation */}
      <div className="mt-10 flex-1 space-y-10">
        {empty && (
          <motion.div
            initial="hidden"
            animate="show"
            variants={{ show: { transition: { staggerChildren: 0.07, delayChildren: 0.35 } } }}
          >
            <motion.p
              variants={{ hidden: { opacity: 0, y: 10 }, show: { opacity: 1, y: 0, transition: { duration: 0.5, ease: EASE_OUT } } }}
              className="max-w-[46ch] font-display text-xl italic leading-relaxed text-ink-mute"
              style={{ fontVariationSettings: '"opsz" 28' }}
            >
              Everything you've read, watched, and received — kept quietly, on this machine, waiting to be asked.
            </motion.p>
            <div className="mt-8 flex flex-wrap gap-2.5">
              {SUGGESTIONS.map((suggestion) => (
                <motion.button
                  key={suggestion}
                  type="button"
                  variants={{ hidden: { opacity: 0, y: 8 }, show: { opacity: 1, y: 0, transition: { duration: 0.4, ease: EASE_OUT } } }}
                  onClick={() => send(suggestion)}
                  className="btn-ghost text-left font-mono text-xs normal-case"
                >
                  {suggestion}
                </motion.button>
              ))}
            </div>
          </motion.div>
        )}

        {messages.map((message) =>
          message.role === 'user' ? (
            <UserMessage key={message.id} text={message.text} />
          ) : (
            <EchoAnswer key={message.id} message={message} />
          ),
        )}

        <AnimatePresence>{busy && <ThinkingIndicator key="thinking" />}</AnimatePresence>
        <div ref={endRef} />
      </div>

      {/* Composer — pinned to the reading line */}
      <form
        onSubmit={(event) => {
          event.preventDefault()
          void send()
        }}
        className="sticky bottom-20 mt-10 lg:bottom-6"
      >
        <div className="flex items-center gap-3 rounded-card border border-white/[0.08] bg-night-900/90 p-2 pl-4 shadow-raise backdrop-blur-study transition focus-within:border-ember-500/40 focus-within:shadow-lamp">
          <input
            ref={inputRef}
            value={input}
            onChange={(event) => setInput(event.target.value)}
            placeholder="ask anything you've seen before…"
            aria-label="Ask your memory"
            disabled={busy}
            className="flex-1 bg-transparent text-[15px] text-ink placeholder:font-mono placeholder:text-xs placeholder:text-ink-faint focus:outline-none disabled:opacity-50"
          />
          <button type="submit" disabled={busy || !input.trim()} className="btn-primary flex items-center gap-2 disabled:opacity-40" aria-label="Send">
            <span className="hidden sm:inline">Ask</span>
            <CornerDownLeft size={15} aria-hidden="true" />
          </button>
        </div>
      </form>
    </div>
  )
}
