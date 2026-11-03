import { useSearchParams } from 'react-router-dom'
import { motion } from 'framer-motion'
import { Moon, Waves } from 'lucide-react'
import PageTitle from '../components/PageTitle'
import ViewSwitch from '../components/ViewSwitch'
import type { ViewOption } from '../components/ViewSwitch'
import ReflectionsView from './ReflectionsView'
import TimeView from './TimeView'

const EASE_OUT = [0.22, 1, 0.36, 1] as const

type PatternsView = 'time' | 'reflections'

const VIEWS: ViewOption<PatternsView>[] = [
  { id: 'time', label: 'Time', icon: Waves },
  { id: 'reflections', label: 'Reflections', icon: Moon },
]

const TITLES: Record<PatternsView, string> = {
  time: 'Where the time went.',
  reflections: 'In your own words.',
}

/** The view in the address (`/patterns?view=reflections`); anything else, or nothing, is the time view. */
function viewFromParam(value: string | null): PatternsView {
  return value === 'reflections' ? 'reflections' : 'time'
}

/**
 * Patterns: two views of how time went. "Time" is the numbers (day, week, categories); "Reflections"
 * is the moments the user flagged themselves. Both stay mounted once the page opens, so a day or week
 * picked in "Time" is still there after a look at "Reflections".
 */
export default function PatternsPage() {
  const [params, setParams] = useSearchParams()
  const view = viewFromParam(params.get('view'))

  function changeView(next: PatternsView) {
    // The time view is the plain address; a view switch replaces the entry, so Back leaves the page.
    setParams(next === 'time' ? {} : { view: next }, { replace: true })
  }

  return (
    <div>
      <PageTitle
        eyebrow="patterns"
        title={TITLES[view]}
        aside={<ViewSwitch name="patterns" label="Patterns view" options={VIEWS} value={view} onChange={changeView} />}
      />

      <div className="mt-8">
        {VIEWS.map((option) => {
          const active = option.id === view
          return (
            <motion.div
              key={option.id}
              role="tabpanel"
              id={`patterns-panel-${option.id}`}
              aria-labelledby={`patterns-tab-${option.id}`}
              hidden={!active}
              initial={false}
              animate={active ? { opacity: 1, y: 0 } : { opacity: 0, y: 8 }}
              transition={{ duration: 0.35, ease: EASE_OUT }}
            >
              {option.id === 'time' ? <TimeView /> : <ReflectionsView />}
            </motion.div>
          )
        })}
      </div>
    </div>
  )
}
