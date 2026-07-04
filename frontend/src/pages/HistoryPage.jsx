import React from 'react'

export default function HistoryPage(){
  const items = [
    {id:1, title:'TechCorp interview email', source:'Gmail', when:'2026-06-20'},
    {id:2, title:'OS memory article', source:'Chrome', when:'2026-06-20'},
    {id:3, title:'Project planning video', source:'YouTube', when:'2026-06-22'},
  ]
  return (
    <div>
      <h2 className="text-2xl font-semibold">History</h2>
      <p className="text-sm text-slate-400">Recent items captured by the project history system</p>
      <div className="mt-4 space-y-3">
        {items.map(it=> (
          <div key={it.id} className="rounded-lg border border-white/10 bg-white/5 p-3">
            <div className="flex items-center justify-between">
              <div>
                <p className="font-semibold text-white">{it.title}</p>
                <p className="text-xs text-slate-400">{it.source} • {it.when}</p>
              </div>
              <button className="text-sm text-echo-200">Open</button>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
