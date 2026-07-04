import React from 'react'
import { NavLink } from 'react-router-dom'

export default function Sidebar(){
  const links = [
    {to: '/', label: 'Chat'},
    {to: '/analysis', label: 'Behavior Analysis'},
    {to: '/regret', label: 'Regret'},
    {to: '/history', label: 'History'},
    {to: '/detail', label: 'Detail'},
    {to: '/settings', label: 'Settings'},
  ]
  return (
    <nav className="hidden lg:block">
      <div className="w-48 space-y-4">
        <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
          <p className="text-sm text-slate-400">Efficient Contextualized</p>
          <p className="font-semibold text-white">History Orchestrator</p>
        </div>
        {links.map(l=> (
          <NavLink key={l.to} to={l.to} className={({isActive})=>`block rounded-lg px-3 py-2 ${isActive? 'bg-echo-400/10 border border-echo-400/20 text-echo-100':'text-slate-300'} hover:bg-white/5`}>{l.label}</NavLink>
        ))}
      </div>
    </nav>
  )
}
