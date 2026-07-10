import { motion } from 'framer-motion'
import type { ReactNode } from 'react'

const EASE_OUT = [0.22, 1, 0.36, 1] as const

/**
 * The page's opening statement: mono eyebrow, oversized Fraunces title with a
 * clip reveal, optional aside pinned to the baseline.
 */
export default function PageTitle({
  eyebrow,
  title,
  aside,
}: {
  eyebrow: string
  title: string
  aside?: ReactNode
}) {
  return (
    <header className="flex flex-wrap items-end justify-between gap-x-8 gap-y-3">
      <div className="min-w-0">
        <motion.p
          className="meta"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ duration: 0.4, delay: 0.05 }}
        >
          {eyebrow}
        </motion.p>
        <div className="overflow-hidden">
          <motion.h1
            className="font-display text-title-xl font-medium italic text-ink"
            style={{ fontVariationSettings: '"opsz" 72, "SOFT" 40' }}
            initial={{ y: '105%' }}
            animate={{ y: 0 }}
            transition={{ duration: 0.7, ease: EASE_OUT, delay: 0.1 }}
          >
            {title}
          </motion.h1>
        </div>
      </div>
      {aside && (
        <motion.div
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, ease: EASE_OUT, delay: 0.3 }}
          className="pb-1.5"
        >
          {aside}
        </motion.div>
      )}
    </header>
  )
}
