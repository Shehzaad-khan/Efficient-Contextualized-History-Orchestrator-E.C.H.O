import React, { useMemo, useState } from 'react'
import { activityItems, categorySeed, chatHistorySeed } from '../data/activityData'

export default function AnalysisPage() {
  const [categories, setCategories] = useState(categorySeed)
  const [selectedCategory, setSelectedCategory] = useState(categorySeed[0].name)
  const [newCategory, setNewCategory] = useState('')

  const selectedItems = useMemo(() => {
    return activityItems.filter((item) => item.category === selectedCategory)
  }, [selectedCategory])

  const rankedCategories = useMemo(() => {
    return [...categories].sort((a, b) => b.minutes - a.minutes)
  }, [categories])

  const totalMinutes = rankedCategories.reduce((sum, item) => sum + item.minutes, 0)
  const topCategory = rankedCategories[0]
  const selectedPercent = totalMinutes > 0 ? Math.round(((categories.find((c) => c.name === selectedCategory)?.minutes || 0) / totalMinutes) * 100) : 0

  function addCategory() {
    const name = newCategory.trim()
    if (!name) return
    if (categories.some((item) => item.name.toLowerCase() === name.toLowerCase())) {
      setNewCategory('')
      return
    }

    const updated = [...categories, { name, minutes: 0 }]
    setCategories(updated)
    setSelectedCategory(name)
    setNewCategory('')
  }

  function bumpCategory(name, delta) {
    setCategories((current) =>
      current.map((item) =>
        item.name === name ? { ...item, minutes: Math.max(0, item.minutes + delta) } : item,
      ),
    )
  }

  return (
    <div className="grid grid-cols-1 gap-6 xl:grid-cols-[380px_minmax(0,1fr)]">
      <section className="space-y-4 xl:sticky xl:top-8 xl:self-start">
        <div className="rounded-2xl border border-white/10 bg-white/5 p-5 shadow-glow">
          <div className="flex items-start justify-between gap-4">
            <div>
              <h2 className="text-2xl font-semibold text-white">Behavior analysis</h2>
              <p className="mt-2 text-sm text-slate-300">Track time by category and inspect the matching history in one place.</p>
            </div>
            <div className="rounded-full border border-echo-400/20 bg-echo-400/10 px-3 py-1 text-xs text-echo-100">{totalMinutes} min</div>
          </div>

          <div className="mt-5 grid grid-cols-1 gap-3 sm:grid-cols-3 xl:grid-cols-1">
            <div className="rounded-2xl border border-white/5 bg-slate-900/40 p-4">
              <p className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Tracked minutes</p>
              <p className="mt-2 text-2xl font-semibold text-white">{totalMinutes}</p>
            </div>
            <div className="rounded-2xl border border-white/5 bg-slate-900/40 p-4">
              <p className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Top category</p>
              <p className="mt-2 text-2xl font-semibold text-white">{topCategory ? topCategory.name : 'None'}</p>
            </div>
            <div className="rounded-2xl border border-white/5 bg-slate-900/40 p-4">
              <p className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Selected</p>
              <p className="mt-2 text-2xl font-semibold text-white">{selectedCategory}</p>
            </div>
          </div>

          <div className="mt-4 rounded-2xl border border-white/5 bg-slate-900/40 p-4">
            <p className="text-sm text-slate-300">{selectedCategory} currently accounts for {selectedPercent}% of tracked time.</p>
          </div>
        </div>

        <div className="rounded-2xl border border-white/10 bg-white/5 p-4 shadow-glow">
          <div className="flex items-center justify-between">
            <h3 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-400">Categories</h3>
            <p className="text-xs text-slate-400">Click to view details</p>
          </div>

          <div className="mt-4 max-h-[460px] space-y-3 overflow-y-auto pr-1">
            {rankedCategories.map((category) => {
              const percent = totalMinutes > 0 ? Math.round((category.minutes / totalMinutes) * 100) : 0
              const active = category.name === selectedCategory

              return (
                <div
                  key={category.name}
                  role="button"
                  tabIndex={0}
                  onClick={() => setSelectedCategory(category.name)}
                  onKeyDown={(e) => e.key === 'Enter' && setSelectedCategory(category.name)}
                  className={`cursor-pointer rounded-2xl border p-4 text-left transition ${
                    active ? 'border-echo-300/40 bg-echo-400/10 shadow-[0_0_0_1px_rgba(103,232,249,0.15)]' : 'border-white/5 bg-slate-900/40 hover:border-white/10 hover:bg-white/5'
                  }`}
                >
                  <div className="flex items-center justify-between gap-3">
                    <p className="text-sm font-semibold text-white">{category.name}</p>
                    <p className="text-xs text-slate-400">{category.minutes} min</p>
                  </div>
                  <div className="mt-3 h-2 rounded-full bg-white/5">
                    <div className="h-2 rounded-full bg-gradient-to-r from-echo-300 to-ember-300" style={{ width: `${Math.max(percent, category.minutes > 0 ? 8 : 0)}%` }} />
                  </div>
                  <div className="mt-3 flex items-center justify-between gap-3 text-xs text-slate-400">
                    <span>{percent}% of total</span>
                    <div className="flex gap-2">
                      <button type="button" className="rounded-full bg-white/10 px-2 py-1 transition hover:bg-white/15" onClick={(e) => { e.stopPropagation(); bumpCategory(category.name, 15) }}>+15m</button>
                      <button type="button" className="rounded-full bg-white/10 px-2 py-1 transition hover:bg-white/15" onClick={(e) => { e.stopPropagation(); bumpCategory(category.name, -15) }}>-15m</button>
                    </div>
                  </div>
                </div>
              )
            })}
          </div>
        </div>

        <div className="rounded-2xl border border-white/5 bg-slate-900/40 p-4 shadow-glow">
          <p className="text-sm font-semibold text-white">Create your own category</p>
          <div className="mt-3 flex gap-2">
            <input
              value={newCategory}
              onChange={(e) => setNewCategory(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && addCategory()}
              placeholder="Add category like Coding or Health"
              className="flex-1 rounded-full border border-white/10 bg-slate-950/70 px-4 py-2 text-sm text-white focus:outline-none"
            />
            <button onClick={addCategory} className="rounded-full bg-white/10 px-4 py-2 text-sm font-semibold text-white">Add</button>
          </div>
        </div>
      </section>

      <section className="rounded-2xl border border-white/10 bg-white/5 p-6 shadow-glow">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <h3 className="text-2xl font-semibold text-white">{selectedCategory} details</h3>
            <p className="mt-1 text-sm text-slate-300">Selected category items and linked queries are stacked in one clean view.</p>
          </div>
          <div className="rounded-full border border-white/10 bg-slate-900/40 px-3 py-1 text-xs text-slate-300">{selectedItems.length + chatHistorySeed.length} entries</div>
        </div>

        <div className="mt-6 space-y-6">
          <div>
            <div className="flex items-center justify-between gap-4">
              <h4 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-400">Activity items</h4>
              <span className="text-xs text-slate-400">{selectedItems.length} items</span>
            </div>

            <div className="mt-4 space-y-3">
              {selectedItems.map((item) => (
                <article key={item.id} className="flex items-center justify-between gap-4 rounded-2xl border border-white/5 bg-slate-900/40 px-4 py-4">
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center gap-3">
                      <p className="text-xs uppercase tracking-[0.2em] text-slate-400">{item.source}</p>
                      <span className="text-xs text-slate-500">{item.when}</span>
                    </div>
                    <h4 className="mt-2 text-base font-semibold text-white">{item.title}</h4>
                    <p className="mt-1 text-sm leading-6 text-slate-300">{item.detail}</p>
                  </div>
                  <div className="shrink-0 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-300">
                    Item
                  </div>
                </article>
              ))}
            </div>

            {selectedItems.length === 0 && (
              <div className="mt-4 rounded-2xl border border-dashed border-white/10 bg-slate-900/30 p-6 text-sm text-slate-400">
                No activity has been logged in this category yet.
              </div>
            )}
          </div>

          <div className="border-t border-white/10 pt-6">
            <div className="flex items-center justify-between gap-4">
              <h4 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-400">Chat history</h4>
              <p className="text-xs uppercase tracking-[0.2em] text-slate-400">Recent queries</p>
            </div>

            <div className="mt-4 space-y-3">
              {chatHistorySeed.map((chat) => (
                <article key={chat.id} className="flex items-start justify-between gap-4 rounded-2xl border border-white/5 bg-slate-900/40 px-4 py-4">
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center gap-3">
                      <p className="text-xs uppercase tracking-[0.2em] text-slate-400">Asked on {chat.source}</p>
                      <span className="rounded-full border border-white/10 bg-white/5 px-2 py-1 text-[11px] text-slate-300">Matched</span>
                    </div>
                    <p className="mt-3 text-sm font-medium text-white">{chat.prompt}</p>
                    <p className="mt-2 text-sm leading-6 text-slate-300">{chat.response}</p>
                  </div>
                </article>
              ))}
            </div>
          </div>
        </div>
      </section>
    </div>
  )
}
