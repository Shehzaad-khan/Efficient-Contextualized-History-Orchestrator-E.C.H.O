import React from 'react'

export default function DetailPage(){
  return (
    <div>
      <h2 className="text-2xl font-semibold">Detail</h2>
      <p className="text-sm text-slate-400">Full view of a selected memory item with metadata and snippets.</p>

      <div className="mt-4 rounded-lg border border-white/10 bg-white/5 p-4">
        <p className="font-semibold">OS memory management article</p>
        <p className="text-xs text-slate-400">Chrome • 2026-06-20</p>
        <div className="mt-3 text-sm text-slate-300">
          <p>Snippet: Paging divides memory into fixed-size blocks called pages...</p>
        </div>
      </div>
    </div>
  )
}
