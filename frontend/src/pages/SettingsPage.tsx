import { useEffect, useState } from 'react'
import { motion } from 'framer-motion'
import PageTitle from '../components/PageTitle'
import Switch from '../components/Switch'
import {
  fetchCaptureSettings,
  isDemoMode,
  onDemoModeChange,
  setReminderSettings,
  updateCaptureSettings,
} from '../lib/api'

const EASE_OUT = [0.22, 1, 0.36, 1] as const

interface LocalSettings {
  indexing: boolean
  gmailPolling: boolean
  chromeCapture: boolean
  youtubeCapture: boolean
  regretReminders: boolean
}

const STORAGE_KEY = 'echo-settings'

const DEFAULTS: LocalSettings = {
  indexing: true,
  gmailPolling: true,
  chromeCapture: true,
  youtubeCapture: true,
  regretReminders: true,
}

function loadSettings(): LocalSettings {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY)
    return stored ? { ...DEFAULTS, ...(JSON.parse(stored) as Partial<LocalSettings>) } : DEFAULTS
  } catch {
    return DEFAULTS
  }
}

function SettingRow({
  title,
  description,
  checked,
  onChange,
}: {
  title: string
  description: string
  checked: boolean
  onChange: (next: boolean) => void
}) {
  return (
    <div className="flex items-center justify-between gap-6 border-b border-white/[0.05] py-4 last:border-0">
      <div className="min-w-0">
        <p className="text-sm font-medium text-ink">{title}</p>
        <p className="mt-1 text-xs leading-5 text-ink-mute">{description}</p>
      </div>
      <Switch checked={checked} onChange={onChange} label={title} />
    </div>
  )
}

// Capture toggles map to the backend's Redis-backed /settings; indexing is the
// one local-only toggle (the ENP worker has no runtime switch).
const CAPTURE_KEY_MAP = {
  gmailPolling: 'gmail_enabled',
  chromeCapture: 'chrome_enabled',
  youtubeCapture: 'youtube_enabled',
} as const

export default function SettingsPage() {
  const [settings, setSettings] = useState<LocalSettings>(loadSettings)
  const [demo, setDemo] = useState(isDemoMode())

  useEffect(() => onDemoModeChange(setDemo), [])

  // Hydrate capture toggles from the backend — it is the source of truth
  // for what the connectors actually enforce.
  useEffect(() => {
    let cancelled = false
    void fetchCaptureSettings().then((remote) => {
      if (cancelled || isDemoMode()) return
      setSettings((current) => ({
        ...current,
        gmailPolling: remote.gmail_enabled,
        chromeCapture: remote.chrome_enabled,
        youtubeCapture: remote.youtube_enabled,
      }))
    })
    return () => {
      cancelled = true
    }
  }, [])

  function update<K extends keyof LocalSettings>(key: K, value: LocalSettings[K]) {
    setSettings((current) => {
      const next = { ...current, [key]: value }
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next))
      return next
    })
    if (key === 'regretReminders') void setReminderSettings(Boolean(value))
    if (key in CAPTURE_KEY_MAP) {
      const backendKey = CAPTURE_KEY_MAP[key as keyof typeof CAPTURE_KEY_MAP]
      void updateCaptureSettings({ [backendKey]: Boolean(value) })
    }
  }

  const apiBase = (import.meta.env.VITE_API_BASE as string | undefined) ?? 'http://localhost:8000'

  return (
    <div>
      <PageTitle eyebrow="settings" title="The house rules." aside={<p className="meta">single user · this machine only</p>} />

      <div className="mt-10 grid grid-cols-1 gap-8 lg:grid-cols-[1.3fr_1fr]">
        <div className="space-y-8">
          <motion.section
            className="card px-6 py-2"
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, ease: EASE_OUT, delay: 0.2 }}
            aria-labelledby="capture-heading"
          >
            <h2 id="capture-heading" className="meta pt-5">
              capture
            </h2>
            <SettingRow
              title="Memory indexing"
              description="Background enrichment: cleaning, classification, and embeddings for search."
              checked={settings.indexing}
              onChange={(value) => update('indexing', value)}
            />
            <SettingRow
              title="Gmail polling"
              description="Every email is kept on arrival — reading time counts only after you open it."
              checked={settings.gmailPolling}
              onChange={(value) => update('gmailPolling', value)}
            />
            <SettingRow
              title="Chrome capture"
              description="Pages must earn their place: real reading time, scrolling, or a return visit."
              checked={settings.chromeCapture}
              onChange={(value) => update('chromeCapture', value)}
            />
            <SettingRow
              title="YouTube capture"
              description="Videos are kept after 20 focused seconds or a deliberate interaction."
              checked={settings.youtubeCapture}
              onChange={(value) => update('youtubeCapture', value)}
            />
          </motion.section>

          <motion.section
            className="card px-6 py-2"
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, ease: EASE_OUT, delay: 0.3 }}
            aria-labelledby="reflection-heading"
          >
            <h2 id="reflection-heading" className="meta pt-5">
              reflection
            </h2>
            <SettingRow
              title="Regret reminders"
              description="At most two a day, only your own words, never blocking anything."
              checked={settings.regretReminders}
              onChange={(value) => update('regretReminders', value)}
            />
          </motion.section>
        </div>

        <div className="space-y-8">
          <motion.section
            className="card p-6"
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, ease: EASE_OUT, delay: 0.35 }}
            aria-labelledby="connection-heading"
          >
            <h2 id="connection-heading" className="meta mb-4">
              connection
            </h2>
            <dl className="space-y-3">
              <div className="flex items-center justify-between gap-4">
                <dt className="text-sm text-ink-soft">Backend</dt>
                <dd className="font-mono text-xs text-ink-mute">{apiBase}</dd>
              </div>
              <div className="flex items-center justify-between gap-4">
                <dt className="text-sm text-ink-soft">Status</dt>
                <dd className={`font-mono text-xs ${demo ? 'text-ember-400' : 'text-moss'}`}>
                  {demo ? 'offline — showing demo data' : 'connected'}
                </dd>
              </div>
            </dl>
          </motion.section>

          <motion.section
            className="card border-ember-500/10 p-6"
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, ease: EASE_OUT, delay: 0.4 }}
            aria-labelledby="promise-heading"
          >
            <h2 id="promise-heading" className="meta mb-4">
              the promise
            </h2>
            <ul className="space-y-2.5 text-sm leading-6 text-ink-mute">
              <li>— Incognito is never tracked.</li>
              <li>— Nothing you type is captured.</li>
              <li>— App content (Slack, Notion, Jira) is never read.</li>
              <li>— Only your question and short snippets ever reach an LLM.</li>
              <li>— Everything else lives and dies on this machine.</li>
            </ul>
          </motion.section>
        </div>
      </div>
    </div>
  )
}
