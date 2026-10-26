import { useCallback, useEffect, useRef, useState } from 'react'
import { History } from 'lucide-react'
import { fetchSessionMessages, fetchSessions } from '../lib/api'
import { formatWhen } from '../lib/format'
import type { SessionMessage, SessionSummary } from '../lib/types'

/**
 * The past-chats list on the Recall page (A27). A button opens a panel with the
 * chats, newest first; picking one loads its messages and hands them to the page,
 * which keeps sending that session id so the conversation continues.
 *
 * The list is fetched each time the panel opens, so it is never stale.
 */

type ListState =
  | { status: 'loading' }
  | { status: 'ready'; sessions: SessionSummary[] }
  | { status: 'error' }

interface PastChatsProps {
  activeSessionId: string | null
  disabled: boolean
  onOpen: (sessionId: string, messages: SessionMessage[]) => void
}

export default function PastChats({ activeSessionId, disabled, onOpen }: PastChatsProps) {
  const [open, setOpen] = useState(false)
  const [list, setList] = useState<ListState>({ status: 'loading' })
  const [openingId, setOpeningId] = useState<string | null>(null)
  const [openError, setOpenError] = useState(false)
  const rootRef = useRef<HTMLDivElement>(null)
  const buttonRef = useRef<HTMLButtonElement>(null)

  const close = useCallback(() => {
    setOpen(false)
    setOpenError(false)
  }, [])

  // Load the list on every open.
  useEffect(() => {
    if (!open) return
    let cancelled = false
    setList({ status: 'loading' })
    fetchSessions()
      .then((sessions) => {
        if (!cancelled) setList({ status: 'ready', sessions })
      })
      .catch(() => {
        if (!cancelled) setList({ status: 'error' })
      })
    return () => {
      cancelled = true
    }
  }, [open])

  // Click outside or Escape closes the panel.
  useEffect(() => {
    if (!open) return
    function onPointerDown(event: MouseEvent) {
      if (rootRef.current && !rootRef.current.contains(event.target as Node)) close()
    }
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        close()
        buttonRef.current?.focus()
      }
    }
    document.addEventListener('mousedown', onPointerDown)
    document.addEventListener('keydown', onKeyDown)
    return () => {
      document.removeEventListener('mousedown', onPointerDown)
      document.removeEventListener('keydown', onKeyDown)
    }
  }, [open, close])

  async function openChat(sessionId: string) {
    if (openingId) return
    setOpeningId(sessionId)
    setOpenError(false)
    try {
      const chat = await fetchSessionMessages(sessionId)
      onOpen(chat.session_id, chat.messages)
      close()
    } catch {
      setOpenError(true)
    } finally {
      setOpeningId(null)
    }
  }

  return (
    <div ref={rootRef} className="relative">
      <button
        ref={buttonRef}
        type="button"
        onClick={() => (open ? close() : setOpen(true))}
        disabled={disabled}
        aria-expanded={open}
        aria-controls="past-chats-panel"
        className="btn-ghost flex items-center gap-2 font-mono text-xs normal-case disabled:opacity-40"
      >
        <History size={13} aria-hidden="true" />
        Past chats
      </button>

      {open && (
        <div
          id="past-chats-panel"
          role="dialog"
          aria-label="Past chats"
          className="absolute right-0 top-full z-30 mt-2 w-[min(22rem,90vw)] rounded-card border border-white/[0.08] bg-night-900 p-2 shadow-raise"
        >
          {list.status === 'loading' && <p className="px-3 py-3 font-mono text-xs text-ink-mute">loading…</p>}
          {list.status === 'error' && (
            <p className="px-3 py-3 text-sm text-ink-soft">Couldn't load your chats. Try again in a moment.</p>
          )}
          {list.status === 'ready' && list.sessions.length === 0 && (
            <p className="px-3 py-3 text-sm text-ink-soft">No past chats yet. Ask something and it will show up here.</p>
          )}
          {list.status === 'ready' && list.sessions.length > 0 && (
            <ul className="max-h-[60vh] space-y-0.5 overflow-y-auto">
              {list.sessions.map((session) => {
                const active = session.session_id === activeSessionId
                return (
                  <li key={session.session_id}>
                    <button
                      type="button"
                      onClick={() => void openChat(session.session_id)}
                      disabled={openingId !== null}
                      aria-current={active ? 'true' : undefined}
                      className={`w-full rounded-card px-3 py-2 text-left transition hover:bg-night-800 focus-visible:outline focus-visible:outline-1 focus-visible:outline-ember-400 disabled:opacity-60 ${
                        active ? 'bg-night-800' : ''
                      }`}
                    >
                      <span className="line-clamp-2 block text-[14px] leading-5 text-ink">{session.title}</span>
                      <span className="mt-1 block font-mono text-[11px] text-ink-mute">
                        {formatWhen(session.last_active)} · {session.turns} {session.turns === 1 ? 'question' : 'questions'}
                        {active ? ' · open now' : ''}
                      </span>
                    </button>
                  </li>
                )
              })}
            </ul>
          )}
          {openError && (
            <p role="alert" className="px-3 pb-2 pt-3 text-sm text-ink-soft">
              That chat could not be opened. It may have been removed.
            </p>
          )}
        </div>
      )}
    </div>
  )
}
