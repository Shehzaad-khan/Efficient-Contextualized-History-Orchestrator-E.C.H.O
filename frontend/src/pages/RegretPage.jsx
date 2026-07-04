import React, { useEffect, useMemo, useState } from 'react'
import { loadRegrets } from '../data/regretData'

export default function RegretPage() {
  const [regrets, setRegrets] = useState([])

  useEffect(() => {
    setRegrets(loadRegrets())
  }, [])

  const grouped = useMemo(() => {
    return regrets.reduce((accumulator, item) => {
      accumulator[item.category] = accumulator[item.category] || []
      accumulator[item.category].push(item)
      return accumulator
    }, {})
  }, [regrets])

  const categories = Object.entries(grouped)

  return (
    <div className="grid grid-cols-1 gap-6 xl:grid-cols-[320px_minmax(0,1fr)]">
      <section className="rounded-2xl border border-white/10 bg-white/5 p-5 shadow-glow xl:sticky xl:top-8 xl:self-start">
        <h2 className="text-2xl font-semibold text-white">Regret signals</h2>
        <p className="mt-2 text-sm text-slate-300">Captured from chat when the user mentions a mistake, missed task, or something they wish they had handled differently.</p>

        <div className="mt-5 rounded-2xl border border-white/5 bg-slate-900/40 p-4">
          <p className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Stored entries</p>
          <p className="mt-2 text-3xl font-semibold text-white">{regrets.length}</p>
        </div>
      </section>

      <section className="space-y-6 rounded-2xl border border-white/10 bg-white/5 p-6 shadow-glow">
        {categories.map(([category, items]) => (
          <div key={category}>
            <div className="flex items-center justify-between gap-4">
              <h3 className="text-xl font-semibold text-white">{category}</h3>
              <span className="text-xs uppercase tracking-[0.2em] text-slate-400">{items.length} entries</span>
            </div>

            <div className="mt-4 space-y-3">
              {items.map((item) => (
                <article key={item.id} className="flex items-center justify-between gap-4 rounded-2xl border border-white/5 bg-slate-900/40 px-4 py-4">
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center gap-3">
                      <p className="text-xs uppercase tracking-[0.2em] text-slate-400">{item.source}</p>
                      <span className="text-xs text-slate-500">{item.when}</span>
                    </div>
                    <h4 className="mt-2 text-base font-semibold text-white">{item.title}</h4>
                    <p className="mt-1 text-sm leading-6 text-slate-300">{item.detail}</p>
                  </div>
                  <div className="shrink-0 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-300">Regret</div>
                </article>
              ))}
            </div>
          </div>
        ))}

        {regrets.length === 0 && (
          <div className="rounded-2xl border border-dashed border-white/10 bg-slate-900/30 p-6 text-sm text-slate-400">
            No regret signals captured yet.
          </div>
        )}
      </section>
    </div>
  )
}
