import { useEffect, useState } from 'react'
import { motion } from 'framer-motion'
import { Download, X } from 'lucide-react'
import PageTitle from '../components/PageTitle'
import Switch from '../components/Switch'
import {
  downloadExport,
  fetchCaptureSettings,
  isDemoMode,
  onDemoModeChange,
  saveExclusions,
  setReminderSettings,
  updateCaptureSettings,
  type ExclusionLists,
  type RetentionChoice,
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

const RETENTION_OPTIONS: ReadonlyArray<{ value: RetentionChoice; label: string }> = [
  { value: 'forever', label: 'Keep forever' },
  { value: '1y', label: '1 year' },
  { value: '6m', label: '6 months' },
]

type Checked = { value: string } | { error: string }

/** "https://Mail.Example.com/inbox" -> "mail.example.com". Echo matches the site and its subdomains. */
function checkSite(raw: string): Checked {
  const host = raw
    .trim()
    .toLowerCase()
    .replace(/^[a-z][a-z0-9+.-]*:\/\//, '')
    .replace(/[/?#].*$/, '')
    .replace(/:\d+$/, '')
    .replace(/^\*?\./, '')
  if (host === 'localhost' || /^[a-z0-9-]+(\.[a-z0-9-]+)+$/.test(host)) return { value: host }
  return { error: 'Enter a site like example.com.' }
}

/** Echo matches a sender as a piece of the From address, so a very short entry would match nearly everyone. */
function checkSender(raw: string): Checked {
  const sender = raw.trim().toLowerCase()
  if (/\s/.test(sender) || sender.length > 254) return { error: 'Enter part of an address, like newsletter@.' }
  if (sender.length < 3) return { error: 'Use at least 3 characters, so it does not match everyone.' }
  return { value: sender }
}

/** One "keep out" list: a text box, an add button and removable chips. Saves on every change. */
function ExclusionList({
  title,
  placeholder,
  help,
  entries,
  check,
  onSave,
}: {
  title: string
  placeholder: string
  help: string
  entries: string[]
  check: (raw: string) => Checked
  onSave: (next: string[]) => Promise<void>
}) {
  const [input, setInput] = useState('')
  const [message, setMessage] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  async function save(next: string[]): Promise<boolean> {
    setBusy(true)
    setMessage(null)
    try {
      await onSave(next)
      return true
    } catch {
      setMessage('Couldn’t save just now — the list is as it was. Try again in a moment.')
      return false
    } finally {
      setBusy(false)
    }
  }

  async function add() {
    if (!input.trim()) return
    const checked = check(input)
    if ('error' in checked) {
      setMessage(checked.error)
      return
    }
    if (entries.includes(checked.value)) {
      setInput('')
      setMessage('Already on the list.')
      return
    }
    if (await save([...entries, checked.value])) setInput('')
  }

  return (
    <div className="mt-5">
      <label htmlFor={`keep-out-${title}`} className="text-sm font-medium text-ink">
        {title}
      </label>
      <p className="mt-1 text-xs leading-5 text-ink-mute">{help}</p>
      <div className="mt-2 flex gap-2">
        <input
          id={`keep-out-${title}`}
          value={input}
          onChange={(event) => {
            setInput(event.target.value)
            setMessage(null)
          }}
          onKeyDown={(event) => {
            if (event.key === 'Enter') void add()
          }}
          placeholder={placeholder}
          maxLength={254}
          className="input-study min-w-0 flex-1 px-3 py-1.5 text-xs"
        />
        <button type="button" onClick={() => void add()} disabled={busy} className="btn-ghost px-4 py-1.5 text-xs disabled:opacity-50">
          {busy ? 'saving…' : 'add'}
        </button>
      </div>
      {message && (
        <p role="alert" className="mt-2 text-xs leading-5 text-ink-soft">
          {message}
        </p>
      )}
      {entries.length > 0 && (
        <ul className="mt-3 flex flex-wrap gap-2" aria-label={`${title} kept out`}>
          {entries.map((entry) => (
            <li key={entry} className="inline-flex items-center gap-1.5 rounded-pill border border-white/[0.08] bg-night-800 py-1 pl-3 pr-1.5 font-mono text-xs text-ink-soft">
              {entry}
              <button
                type="button"
                onClick={() => void save(entries.filter((other) => other !== entry))}
                disabled={busy}
                aria-label={`Stop keeping out ${entry}`}
                className="rounded-full p-1 text-ink-mute transition hover:text-ink disabled:opacity-50"
              >
                <X size={11} aria-hidden="true" />
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

export default function SettingsPage() {
  const [settings, setSettings] = useState<LocalSettings>(loadSettings)
  const [retention, setRetention] = useState<RetentionChoice>('forever')
  const [excluded, setExcluded] = useState<ExclusionLists>({ excluded_domains: [], excluded_senders: [] })
  const [exporting, setExporting] = useState<'json' | 'csv' | null>(null)
  const [exportMessage, setExportMessage] = useState<string | null>(null)
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
      setRetention(remote.retention)
      setExcluded({ excluded_domains: remote.excluded_domains, excluded_senders: remote.excluded_senders })
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

  function chooseRetention(value: RetentionChoice) {
    setRetention(value)
    void updateCaptureSettings({ retention: value })
  }

  /** Save one list and show what the server now holds (it lowercases, de-duplicates and sorts). Throws on failure. */
  async function saveList(key: keyof ExclusionLists, next: string[]) {
    const saved = await saveExclusions({ [key]: next })
    setExcluded(saved)
  }

  async function exportData(format: 'json' | 'csv') {
    setExporting(format)
    setExportMessage(null)
    try {
      await downloadExport(format)
      setExportMessage('Downloaded. The file holds titles, links and senders, so keep it somewhere private.')
    } catch {
      setExportMessage('Couldn’t download your data just now. Try again in a moment.')
    } finally {
      setExporting(null)
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
            className="card px-6 py-5"
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, ease: EASE_OUT, delay: 0.25 }}
            aria-labelledby="retention-heading"
          >
            <h2 id="retention-heading" className="meta">
              retention
            </h2>
            <p className="mt-3 text-sm font-medium text-ink">How long Echo keeps your memories</p>
            <p className="mt-1 text-xs leading-5 text-ink-mute">
              {retention === 'forever'
                ? 'Nothing is deleted by age. Recalling old things is the point of Echo.'
                : 'Items older than this are deleted for good — text, search index and cache — the next time the backend starts.'}
            </p>
            <div role="radiogroup" aria-label="Retention period" className="mt-4 flex flex-wrap gap-2">
              {RETENTION_OPTIONS.map((option) => (
                <button
                  key={option.value}
                  type="button"
                  role="radio"
                  aria-checked={retention === option.value}
                  onClick={() => chooseRetention(option.value)}
                  className={`rounded-pill px-4 py-1.5 text-xs transition duration-300 ease-out ${
                    retention === option.value ? 'bg-ember-500 text-ink' : 'bg-night-700 text-ink-mute hover:text-ink'
                  }`}
                >
                  {option.label}
                </button>
              ))}
            </div>
          </motion.section>

          <motion.section
            className="card px-6 py-5"
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, ease: EASE_OUT, delay: 0.28 }}
            aria-labelledby="keep-out-heading"
          >
            <h2 id="keep-out-heading" className="meta">
              keep out
            </h2>
            <p className="mt-3 text-sm font-medium text-ink">Sites and senders Echo never stores</p>
            <p className="mt-1 text-xs leading-5 text-ink-mute">
              Anything from these is skipped entirely: nothing is captured, kept or searchable. Changes apply to new
              activity; what is already stored stays until you delete it from the Timeline.
            </p>
            <ExclusionList
              title="Sites"
              placeholder="example.com"
              help="A site and all its subdomains: example.com also covers mail.example.com."
              entries={excluded.excluded_domains}
              check={checkSite}
              onSave={(next) => saveList('excluded_domains', next)}
            />
            <ExclusionList
              title="Senders"
              placeholder="newsletter@"
              help="Part of an email address: newsletter@ covers every address that contains it."
              entries={excluded.excluded_senders}
              check={checkSender}
              onSave={(next) => saveList('excluded_senders', next)}
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
            className="card p-6"
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, ease: EASE_OUT, delay: 0.38 }}
            aria-labelledby="your-data-heading"
          >
            <h2 id="your-data-heading" className="meta mb-4">
              your data
            </h2>
            <p className="text-sm font-medium text-ink">Take a copy with you</p>
            <p className="mt-1 text-xs leading-5 text-ink-mute">
              Every item Echo stored: title, source, times, time spent, and the sender, link or channel. The full
              text, the search index and your Google sign-in are not included.
            </p>
            <div className="mt-4 flex flex-wrap gap-2">
              {(['json', 'csv'] as const).map((format) => (
                <button
                  key={format}
                  type="button"
                  onClick={() => void exportData(format)}
                  disabled={exporting !== null}
                  className="btn-ghost inline-flex items-center gap-2 text-xs disabled:opacity-50"
                >
                  <Download size={13} aria-hidden="true" /> {exporting === format ? 'preparing…' : `download ${format.toUpperCase()}`}
                </button>
              ))}
            </div>
            {exportMessage && (
              <p role="status" className="mt-3 text-xs leading-5 text-ink-soft">
                {exportMessage}
              </p>
            )}
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
