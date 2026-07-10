import { useEffect, useRef } from 'react'

/**
 * Ambient "memory motes" — a slow-drifting particle constellation on 2D
 * canvas. Deliberately not three.js: a dashboard gains atmosphere from this,
 * not geometry, and a rAF canvas costs a fraction of a WebGL context.
 *
 * Respects prefers-reduced-motion (renders nothing) and pauses when the tab
 * is hidden.
 */
const MOTE_COUNT = 64
const LINK_DISTANCE = 110

interface Mote {
  x: number
  y: number
  vx: number
  vy: number
  r: number
  warmth: number // 0..1 — how amber the mote glows
}

export default function ConstellationCanvas() {
  const canvasRef = useRef<HTMLCanvasElement>(null)

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return

    const ctx = canvas.getContext('2d')
    if (!ctx) return

    let width = 0
    let height = 0
    let rafId = 0
    let running = true
    const dpr = Math.min(window.devicePixelRatio || 1, 2)

    const motes: Mote[] = Array.from({ length: MOTE_COUNT }, () => ({
      x: Math.random(),
      y: Math.random(),
      vx: (Math.random() - 0.5) * 0.00012,
      vy: (Math.random() - 0.5) * 0.00009,
      r: 0.7 + Math.random() * 1.3,
      warmth: Math.random(),
    }))

    function resize() {
      if (!canvas) return
      width = canvas.clientWidth
      height = canvas.clientHeight
      canvas.width = Math.round(width * dpr)
      canvas.height = Math.round(height * dpr)
      ctx?.setTransform(dpr, 0, 0, dpr, 0, 0)
    }

    function frame() {
      if (!running || !ctx) return
      ctx.clearRect(0, 0, width, height)

      for (const mote of motes) {
        mote.x = (mote.x + mote.vx + 1) % 1
        mote.y = (mote.y + mote.vy + 1) % 1
      }

      // Links first, motes on top
      ctx.lineWidth = 0.5
      for (let i = 0; i < motes.length; i++) {
        for (let j = i + 1; j < motes.length; j++) {
          const dx = (motes[i].x - motes[j].x) * width
          const dy = (motes[i].y - motes[j].y) * height
          const dist = Math.hypot(dx, dy)
          if (dist < LINK_DISTANCE) {
            const alpha = (1 - dist / LINK_DISTANCE) * 0.09
            ctx.strokeStyle = `rgba(245, 169, 71, ${alpha})`
            ctx.beginPath()
            ctx.moveTo(motes[i].x * width, motes[i].y * height)
            ctx.lineTo(motes[j].x * width, motes[j].y * height)
            ctx.stroke()
          }
        }
      }

      for (const mote of motes) {
        const amber = `rgba(245, 169, 71, ${0.12 + mote.warmth * 0.2})`
        const cream = `rgba(237, 230, 216, ${0.1 + mote.warmth * 0.12})`
        ctx.fillStyle = mote.warmth > 0.65 ? amber : cream
        ctx.beginPath()
        ctx.arc(mote.x * width, mote.y * height, mote.r, 0, Math.PI * 2)
        ctx.fill()
      }

      rafId = requestAnimationFrame(frame)
    }

    function onVisibility() {
      running = document.visibilityState === 'visible'
      if (running) {
        rafId = requestAnimationFrame(frame)
      } else {
        cancelAnimationFrame(rafId)
      }
    }

    resize()
    const observer = new ResizeObserver(resize)
    observer.observe(canvas)
    document.addEventListener('visibilitychange', onVisibility)
    rafId = requestAnimationFrame(frame)

    return () => {
      running = false
      cancelAnimationFrame(rafId)
      observer.disconnect()
      document.removeEventListener('visibilitychange', onVisibility)
    }
  }, [])

  return (
    <canvas
      ref={canvasRef}
      aria-hidden="true"
      className="pointer-events-none fixed inset-0 z-canvas h-full w-full opacity-60"
    />
  )
}
