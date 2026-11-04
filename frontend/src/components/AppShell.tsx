import { useEffect, useState } from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import { motion } from 'framer-motion'
import { Clock3, MessageCircle, Settings2, Target, Waves } from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import Lenis from 'lenis'
import gsap from 'gsap'
import { ScrollTrigger } from 'gsap/ScrollTrigger'
import type { ReactNode } from 'react'
import ConstellationCanvas from './ConstellationCanvas'
import RegretReminderNotice from './RegretReminderNotice'
import { isDemoMode, onDemoModeChange, probeBackend } from '../lib/api'

gsap.registerPlugin(ScrollTrigger)

const EASE_OUT = [0.22, 1, 0.36, 1] as const

interface NavEntry {
  to: string
  label: string
  icon: LucideIcon
  hint: string
}

const MAIN_NAV: NavEntry[] = [
  { to: '/', label: 'Recall', icon: MessageCircle, hint: 'Ask your memory' },
  { to: '/timeline', label: 'Timeline', icon: Clock3, hint: 'What Echo kept' },
  { to: '/patterns', label: 'Patterns', icon: Waves, hint: 'Time & reflections' },
  { to: '/groups', label: 'Groups', icon: Target, hint: 'What you work toward' },
]

// Pinned to the bottom of the desktop rail; the mobile bar lists it last.
const SETTINGS_NAV: NavEntry = { to: '/settings', label: 'Settings', icon: Settings2, hint: 'Privacy & connectors' }

const NAV: NavEntry[] = [...MAIN_NAV, SETTINGS_NAV]

function SidebarLink({ to, label, icon: Icon, hint }: NavEntry) {
  return (
    <NavLink
      to={to}
      end={to === '/'}
      className={({ isActive }) =>
        `group relative flex items-center gap-3 rounded-card px-3 py-2.5 transition duration-200 ease-out ${
          isActive ? 'text-ink' : 'text-ink-mute hover:bg-white/[0.03] hover:text-ink-soft'
        }`
      }
    >
      {({ isActive }) => (
        <>
          {isActive && (
            <motion.span
              layoutId="nav-tick"
              aria-hidden="true"
              className="absolute left-0 top-1/2 h-5 w-0.5 -translate-y-1/2 rounded-full bg-ember-500 shadow-thread"
              transition={{ duration: 0.35, ease: EASE_OUT }}
            />
          )}
          <Icon size={16} strokeWidth={1.75} aria-hidden="true" className={isActive ? 'text-ember-400' : ''} />
          <span className="text-sm font-medium">{label}</span>
          <span className="meta ml-auto hidden text-[9px] normal-case tracking-normal text-ink-faint opacity-0 transition group-hover:opacity-100 xl:block">
            {hint}
          </span>
        </>
      )}
    </NavLink>
  )
}

function DemoBadge() {
  const [demo, setDemo] = useState(isDemoMode())
  useEffect(() => onDemoModeChange(setDemo), [])
  if (!demo) return null
  return (
    <div
      role="status"
      className="rounded-pill border border-ember-500/30 bg-ember-500/10 px-3 py-1 font-mono text-[10px] uppercase tracking-[0.18em] text-ember-300"
    >
      demo data — backend offline
    </div>
  )
}

export default function AppShell({ children }: { children: ReactNode }) {
  const location = useLocation()

  // /health is the single source of truth for the demo badge: probe once on
  // mount, then poll so the app self-heals when the backend comes back (or
  // correctly flips to demo when it goes away) without needing a page reload.
  useEffect(() => {
    void probeBackend()
    const id = window.setInterval(() => void probeBackend(), 15000)
    return () => window.clearInterval(id)
  }, [])

  // Lenis smooth scrolling, wired into ScrollTrigger. Skipped entirely for
  // reduced-motion users — native scroll is the accessible baseline.
  useEffect(() => {
    if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return
    const lenis = new Lenis({ lerp: 0.12 })
    lenis.on('scroll', ScrollTrigger.update)
    const tick = (time: number) => lenis.raf(time * 1000)
    gsap.ticker.add(tick)
    gsap.ticker.lagSmoothing(0)
    return () => {
      gsap.ticker.remove(tick)
      lenis.destroy()
    }
  }, [])

  // New page starts at the top (Lenis keeps scroll position otherwise).
  useEffect(() => {
    window.scrollTo({ top: 0, behavior: 'instant' as ScrollBehavior })
  }, [location.pathname])

  return (
    <div className="grain relative min-h-screen">
      <ConstellationCanvas />

      {/* ── Left rail (desktop) ─────────────────────────────────────────── */}
      <aside className="fixed inset-y-0 left-0 z-nav hidden w-56 flex-col border-r border-white/[0.05] bg-night-950/70 backdrop-blur-study lg:flex">
        <div className="px-6 pb-8 pt-8">
          <p className="font-display text-3xl font-semibold italic tracking-tight text-ink" style={{ fontVariationSettings: '"opsz" 40' }}>
            Echo
          </p>
          <p className="meta mt-1.5">remember what matters</p>
        </div>

        <nav aria-label="Primary" className="flex flex-1 flex-col gap-1 px-3">
          {MAIN_NAV.map((entry) => (
            <SidebarLink key={entry.to} {...entry} />
          ))}
        </nav>

        <div className="flex flex-col gap-4 px-3 pb-6">
          <SidebarLink {...SETTINGS_NAV} />
          <div className="px-3">
            <DemoBadge />
          </div>
        </div>
      </aside>

      {/* ── Bottom bar (mobile) ─────────────────────────────────────────── */}
      <nav
        aria-label="Primary"
        className="fixed inset-x-0 bottom-0 z-nav flex items-stretch justify-around border-t border-white/[0.06] bg-night-950/85 backdrop-blur-study lg:hidden"
      >
        {NAV.map(({ to, label, icon: Icon }) => (
          <NavLink
            key={to}
            to={to}
            end={to === '/'}
            className={({ isActive }) =>
              `flex min-w-[56px] flex-col items-center gap-1 px-2 pb-[max(0.6rem,env(safe-area-inset-bottom))] pt-2.5 transition ${
                isActive ? 'text-ember-400' : 'text-ink-mute'
              }`
            }
          >
            <Icon size={18} strokeWidth={1.75} aria-hidden="true" />
            <span className="font-mono text-[9px] uppercase tracking-wider">{label}</span>
          </NavLink>
        ))}
      </nav>

      <RegretReminderNotice />

      {/* ── Page ─────────────────────────────────────────────────────────── */}
      <div className="relative z-page pb-24 lg:pb-0 lg:pl-56">
        {/* A CSS fade, keyed by the address so each page plays it once. It used to be a framer-motion exit then
            enter (AnimatePresence "wait"): when that animation stalled, the new page stayed invisible until a refresh. */}
        <main
          key={location.pathname}
          className="page-enter mx-auto min-h-screen w-full max-w-6xl px-5 py-8 sm:px-8 lg:px-12 lg:py-12"
        >
          {children}
        </main>
      </div>
    </div>
  )
}
