import { useEffect, useRef } from 'react'
import gsap from 'gsap'

/**
 * GSAP-driven number counter — tweens a plain object and writes formatted
 * text each tick. GSAP wins here over spring physics: exact duration, exact
 * easing, no overshoot on data values.
 */
export default function Counter({
  value,
  format = (v: number) => String(Math.round(v)),
  className,
}: {
  value: number
  format?: (value: number) => string
  className?: string
}) {
  const spanRef = useRef<HTMLSpanElement>(null)
  const previous = useRef(0)

  useEffect(() => {
    const span = spanRef.current
    if (!span) return
    const state = { v: previous.current }
    const tween = gsap.to(state, {
      v: value,
      duration: 1.1,
      ease: 'power3.out',
      onUpdate: () => {
        span.textContent = format(state.v)
      },
    })
    previous.current = value
    return () => {
      tween.kill()
    }
  }, [value, format])

  return (
    <span ref={spanRef} className={className}>
      {format(0)}
    </span>
  )
}
