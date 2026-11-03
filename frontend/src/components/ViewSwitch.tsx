import { useRef } from 'react'
import type { KeyboardEvent } from 'react'
import { motion } from 'framer-motion'
import type { LucideIcon } from 'lucide-react'

const EASE_OUT = [0.22, 1, 0.36, 1] as const

export interface ViewOption<Id extends string> {
  id: Id
  label: string
  icon?: LucideIcon
}

/**
 * A two-or-three-way switch between views of one page, drawn as a pill with a sliding highlight and
 * an icon on each view.
 * It is a WAI-ARIA tab list: arrow keys move between the views, Home and End jump to the ends. The
 * panels it controls must carry `id={`${name}-panel-${id}`}`.
 */
export default function ViewSwitch<Id extends string>({
  name,
  label,
  options,
  value,
  onChange,
}: {
  /** Prefix for the element ids, so two switches on one page never share one. */
  name: string
  label: string
  options: ViewOption<Id>[]
  value: Id
  onChange: (next: Id) => void
}) {
  const refs = useRef<Record<string, HTMLButtonElement | null>>({})

  function onKeyDown(event: KeyboardEvent<HTMLButtonElement>, index: number) {
    const last = options.length - 1
    const target =
      event.key === 'ArrowRight' ? (index === last ? 0 : index + 1)
      : event.key === 'ArrowLeft' ? (index === 0 ? last : index - 1)
      : event.key === 'Home' ? 0
      : event.key === 'End' ? last
      : null
    if (target === null) return
    event.preventDefault()
    const next = options[target]
    onChange(next.id)
    refs.current[next.id]?.focus()
  }

  return (
    <div
      role="tablist"
      aria-label={label}
      className="inline-flex items-center gap-1 rounded-pill border border-ember-500/40 bg-night-850 p-1.5 shadow-raise"
    >
      {options.map((option, index) => {
        const selected = option.id === value
        const Icon = option.icon
        return (
          <button
            key={option.id}
            ref={(node) => {
              refs.current[option.id] = node
            }}
            type="button"
            role="tab"
            id={`${name}-tab-${option.id}`}
            aria-selected={selected}
            aria-controls={`${name}-panel-${option.id}`}
            tabIndex={selected ? 0 : -1}
            onClick={() => onChange(option.id)}
            onKeyDown={(event) => onKeyDown(event, index)}
            className={`relative flex items-center gap-2 rounded-pill px-5 py-2.5 text-[15px] font-medium transition-colors duration-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember-400/60 ${
              selected ? 'text-night-950' : 'text-ink-soft hover:bg-white/[0.05] hover:text-ink'
            }`}
          >
            {selected && (
              <motion.span
                layoutId={`${name}-pill`}
                aria-hidden="true"
                className="absolute inset-0 rounded-pill bg-ember-500 shadow-[0_0_22px_rgba(245,169,71,0.4)]"
                transition={{ duration: 0.3, ease: EASE_OUT }}
              />
            )}
            {Icon && <Icon size={16} strokeWidth={1.9} aria-hidden="true" className="relative" />}
            <span className="relative">{option.label}</span>
          </button>
        )
      })}
    </div>
  )
}
