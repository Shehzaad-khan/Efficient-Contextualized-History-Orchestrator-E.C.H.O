import React from 'react'

export default function ChatBubble({ from, text, extra }) {
  const isUser = from === 'user'
  const isAssistant = from === 'assistant'
  const base = 'rounded-2xl p-3 max-w-[85%]'
  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}>
      <div className={`${base} ${isUser ? 'bg-emerald-300/10 text-emerald-200' : 'bg-slate-900/60 text-slate-200'}`}>
        <div className="text-sm">{text}</div>
        {extra && (
          <div className="mt-2 text-xs text-slate-400">
            Sources:
            <ul className="mt-1 list-disc pl-5">
              {extra.map((s, i) => (
                <li key={i}>{s.type}: {s.title} ({s.when})</li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </div>
  )
}
