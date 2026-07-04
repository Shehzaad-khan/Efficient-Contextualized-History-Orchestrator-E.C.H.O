import React, { useMemo, useState } from 'react'

export default function AnalysisPanel({ categories = [], onCategoriesChange }) {
  const [newCategory, setNewCategory] = useState('')

  const rankedCategories = useMemo(() => {
    return [...categories].sort((a, b) => b.minutes - a.minutes)
  }, [categories])

  const topCategory = rankedCategories[0]
  const totalMinutes = rankedCategories.reduce((sum, item) => sum + item.minutes, 0)

  function addCategory() {
    const name = newCategory.trim()
    if (!name) return
    const exists = categories.some((item) => item.name.toLowerCase() === name.toLowerCase())
    if (exists) {
      setNewCategory('')
      return
    }

    onCategoriesChange?.([...categories, { name, minutes: 0 }])
    setNewCategory('')
  }

  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 p-4 shadow-glow">
      <h3 className="text-lg font-semibold text-white">Behavior analysis</h3>
      <p className="mt-2 text-sm text-slate-300">Where your time is going across categories</p>

      <div className="mt-4 rounded-xl border border-white/5 bg-slate-900/40 p-4">
        <p className="text-xs uppercase tracking-[0.2em] text-slate-400">Total time tracked</p>
        <p className="mt-2 text-3xl font-semibold text-white">{totalMinutes} min</p>
        <p className="mt-1 text-sm text-slate-400">
          Highest category: {topCategory ? topCategory.name : 'None'}
        </p>
      </div>

      <div className="mt-4 space-y-3">
        {rankedCategories.map((category) => {
          const percent = totalMinutes > 0 ? Math.round((category.minutes / totalMinutes) * 100) : 0

          return (
            <div key={category.name} className="rounded-lg border border-white/5 bg-slate-900/40 p-3">
              <div className="flex items-center justify-between gap-3">
                <p className="text-sm font-medium text-white">{category.name}</p>
                <p className="text-xs text-slate-400">{category.minutes} min</p>
              </div>
              <div className="mt-2 h-2 rounded-full bg-white/5">
                <div
                  className="h-2 rounded-full bg-gradient-to-r from-echo-300 to-ember-300"
                  style={{ width: `${Math.max(percent, category.minutes > 0 ? 10 : 0)}%` }}
                />
              </div>
              <p className="mt-2 text-xs text-slate-400">{percent}% of tracked time</p>
            </div>
          )
        })}
      </div>

      <div className="mt-4 rounded-xl border border-white/5 bg-slate-900/40 p-3">
        <p className="text-sm font-medium text-white">Create a custom category</p>
        <div className="mt-3 flex gap-2">
          <input
            value={newCategory}
            onChange={(e) => setNewCategory(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && addCategory()}
            placeholder="Add category like Interview or Coding"
            className="flex-1 rounded-full border border-white/10 bg-slate-950/70 px-4 py-2 text-sm text-white focus:outline-none"
          />
          <button onClick={addCategory} className="rounded-full bg-white/10 px-4 py-2 text-sm font-semibold text-white">
            Add
          </button>
        </div>
      </div>
    </div>
  )
}
