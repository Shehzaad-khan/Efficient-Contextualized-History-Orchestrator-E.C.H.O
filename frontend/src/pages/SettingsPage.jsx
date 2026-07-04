import React from 'react'

export default function SettingsPage(){
  return (
    <div>
      <h2 className="text-2xl font-semibold">Settings</h2>
      <p className="text-sm text-slate-400">Local settings for privacy, indexing, and connectors.</p>
      <div className="mt-4 space-y-3">
        <div className="rounded-lg border border-white/10 bg-white/5 p-3">
          <p className="text-sm">Indexing: <strong>ENABLED</strong></p>
        </div>
        <div className="rounded-lg border border-white/10 bg-white/5 p-3">
          <p className="text-sm">Background Gmail Polling: <strong>ENABLED</strong></p>
        </div>
      </div>
    </div>
  )
}
