import React, { useMemo, useState } from 'react'
import ChatBubble from '../components/ChatBubble'
import { buildRegretEntry, isRegretQuery, loadRegrets, saveRegrets } from '../data/regretData'

const initialMessages = [
  { id: 1, from: 'system', text: 'Welcome to Efficient Contextualized History Orchestrator — ask anything about your digital history.' },
]

export default function ChatPage() {
  const [messages, setMessages] = useState(initialMessages)
  const [input, setInput] = useState('')
  const focus = useMemo(() => {
    const text = input.toLowerCase()
    if (text.includes('gmail') || text.includes('email') || text.includes('mail')) return 'Gmail'
    if (text.includes('youtube') || text.includes('video') || text.includes('watch')) return 'YouTube'
    if (text.includes('chrome') || text.includes('web') || text.includes('page') || text.includes('article')) return 'Chrome'
    return 'All'
  }, [input])

  function buildReply(query) {
    const lower = query.toLowerCase()

    if (lower.includes('gmail') || lower.includes('email') || lower.includes('mail')) {
      return {
        text: 'I found a Gmail match: TechCorp interview follow-up with availability instructions.',
        sources: [{ type: 'Gmail', title: 'TechCorp interview follow-up', when: 'Today, 9:12 AM' }],
      }
    }

    if (lower.includes('youtube') || lower.includes('video') || lower.includes('watch')) {
      return {
        text: 'I found a YouTube match: a project planning video about milestones and delivery checkpoints.',
        sources: [{ type: 'YouTube', title: 'Project planning video', when: 'Two days ago' }],
      }
    }

    if (lower.includes('chrome') || lower.includes('web') || lower.includes('page') || lower.includes('article')) {
      return {
        text: 'I found a Chrome match: an OS memory management article you opened after the interview email.',
        sources: [{ type: 'Chrome', title: 'OS memory management article', when: 'Last week' }],
      }
    }

    return {
      text: 'I found the strongest mix across your history: Gmail, Chrome, and YouTube items that fit the question.',
      sources: [
        { type: 'Gmail', title: 'TechCorp interview follow-up', when: 'Today, 9:12 AM' },
        { type: 'Chrome', title: 'OS memory management article', when: 'Last week' },
        { type: 'YouTube', title: 'Project planning video', when: 'Two days ago' },
      ],
    }
  }

  function send() {
    if (!input.trim()) return
    const user = { id: Date.now(), from: 'user', text: input }
    setMessages((m) => [...m, user])
    // fake assistant reply
    setTimeout(() => {
      const replyData = buildReply(input)
      const reply = {
        id: Date.now() + 1,
        from: 'assistant',
        text: replyData.text,
        sources: replyData.sources,
      }
      setMessages((m) => [...m, reply])

      if (isRegretQuery(input)) {
        const current = loadRegrets()
        const next = [buildRegretEntry(input, replyData.sources?.[0]?.type || 'Chat', replyData.text), ...current].slice(0, 25)
        saveRegrets(next)
      }
    }, 700)
    setInput('')
  }

  return (
    <section className="flex flex-col gap-4">
        <div className="flex items-center justify-between">
          <h2 className="text-xl font-semibold">Conversation</h2>
          <div className="text-sm text-slate-400">Focus: {focus}</div>
        </div>

        <div className="flex-1 space-y-3 overflow-auto rounded-2xl border border-white/10 bg-white/5 p-4" style={{height: '56vh'}}>
          {messages.map((m) => (
            <ChatBubble key={m.id} from={m.from} text={m.text} extra={m.sources} />
          ))}
        </div>

        <div className="mt-2 flex gap-3">
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && send()}
            placeholder="Ask something like: 'Show only the Gmail interview email'"
            className="flex-1 rounded-full border border-white/10 bg-slate-900/50 px-4 py-3 text-sm text-white focus:outline-none"
          />
          <button onClick={send} className="rounded-full bg-gradient-to-r from-echo-400 to-ember-400 px-5 py-2 font-semibold text-slate-900">Send</button>
        </div>
    </section>
  )
}
