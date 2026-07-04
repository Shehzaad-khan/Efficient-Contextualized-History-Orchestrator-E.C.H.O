import React from 'react'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Sidebar from './components/Sidebar'
import ChatPage from './pages/ChatPage'
import AnalysisPage from './pages/AnalysisPage'
import RegretPage from './pages/RegretPage'
import HistoryPage from './pages/HistoryPage'
import DetailPage from './pages/DetailPage'
import SettingsPage from './pages/SettingsPage'

function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-[#04111a] text-slate-100">
        <div className="relative isolate overflow-hidden">
          <div className="absolute inset-0 bg-[radial-gradient(circle_at_top,_rgba(6,182,212,0.25),_transparent_42%),linear-gradient(180deg,#06131d_0%,#04111a_48%,#02070d_100%)]" />
          <div className="absolute inset-0 bg-echo-grid bg-[length:22px_22px] opacity-20" />
          <div className="absolute left-1/2 top-0 h-72 w-72 -translate-x-1/2 rounded-full bg-emerald-400/10 blur-3xl" />
          <div className="absolute right-0 top-28 h-80 w-80 rounded-full bg-amber-400/10 blur-3xl" />

          <main className="relative mx-auto flex min-h-screen max-w-7xl flex-col px-6 py-8 lg:px-10">
            <header className="flex items-start justify-start rounded-2xl border border-white/10 bg-white/5 px-5 py-4 backdrop-blur-xl shadow-glow">
              <div className="rounded-full border border-echo-400/20 bg-echo-400/10 px-3 py-1">
                <p className="text-xs font-semibold tracking-[0.3em] text-echo-100">ECHO</p>
              </div>
            </header>

            <div className="mt-8 flex gap-8">
              <Sidebar />
              <div className="flex-1">
                <Routes>
                  <Route path="/" element={<ChatPage />} />
                  <Route path="/analysis" element={<AnalysisPage />} />
                  <Route path="/regret" element={<RegretPage />} />
                  <Route path="/history" element={<HistoryPage />} />
                  <Route path="/detail" element={<DetailPage />} />
                  <Route path="/settings" element={<SettingsPage />} />
                </Routes>
              </div>
            </div>
          </main>
        </div>
      </div>
    </BrowserRouter>
  )
}

export default App
