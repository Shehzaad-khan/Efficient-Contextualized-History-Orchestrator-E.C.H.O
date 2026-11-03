import { useEffect, useState } from 'react'
import type { ReactNode } from 'react'
import { motion } from 'framer-motion'
import { Archive, Ban, Bell, Check, Download, Globe, Hourglass, Mail, ShieldCheck, X, Youtube } from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import PageTitle from '../components/PageTitle'
import ReminderPanel from '../components/ReminderPanel'
import Switch from '../components/Switch'
import {
  downloadExport,
  fetchCaptureSettings,
  fetchReminderStatus,
  isDemoMode,
  onDemoModeChange,
  saveExclusions,
  setReminderSettings,
  updateCaptureSettings,
  type ExclusionLists,
  type RetentionChoice,
} from '../lib/api'
import type { ReminderStatus } from '../lib/types'

const EASE_OUT = [0.22, 1, 0.36, 1] as const

interface LocalSettings {
  gmailPolling: boolean
  chromeCapture: boolean
  youtubeCapture: boolean
  regretReminders: boolean
}

const STORAGE_KEY = 'echo-settings'

const DEFAULTS: LocalSettings = {
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

// Capture toggles map to the backend's Redis-backed /settings.
const CAPTURE_KEY_MAP = {
  gmailPolling: 'gmail_enabled',
  chromeCapture: 'chrome_enabled',
  youtubeCapture: 'youtube_enabled',
} as const

type SourceKey = keyof typeof CAPTURE_KEY_MAP

/** What each source does, and what its switch means in each position, in plain words. */
const SOURCES: ReadonlyArray<{
  key: SourceKey
  icon: LucideIcon
  title: string
  does: string
  on: string
  off: string
}> = [
  {
    key: 'gmailPolling',
    icon: Mail,
    title: 'Gmail',
    does: 'Saves your emails so you can search them later.',
    on: 'Saving new emails.',
    off: 'Paused. New emails are not saved.',
  },
  {
    key: 'chromeCapture',
    icon: Globe,
    title: 'Web pages',
    does: 'Saves the pages you really read in Chrome, not every page you open.',
    on: 'Saving the pages you read.',
    off: 'Paused. New pages are not saved.',
  },
  {
    key: 'youtubeCapture',
    icon: Youtube,
    title: 'YouTube',
    does: 'Saves the videos you watch for about 20 seconds or interact with.',
    on: 'Saving the videos you watch.',
    off: 'Paused. New videos are not saved.',
  },
]

const RETENTION_OPTIONS: ReadonlyArray<{ value: RetentionChoice; label: string; detail: string }> = [
  { value: 'forever', label: 'Keep everything', detail: 'Nothing is deleted because of its age. Best if you want to recall things from long ago.' },
  { value: '1y', label: '1 year', detail: 'Anything older than a year is deleted for good.' },
  { value: '6m', label: '6 months', detail: 'Anything older than six months is deleted for good.' },
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

/** A chapter of the page: an icon, a title in the page's display type, one line saying what it is for. */
function Section({
  icon: Icon,
  title,
  hint,
  delay,
  children,
}: {
  icon: LucideIcon
  title: string
  hint: string
  delay: number
  children: ReactNode
}) {
  const id = `settings-${title.toLowerCase().replace(/[^a-z]+/g, '-')}`
  return (
    <motion.section
      className="mt-14 first:mt-10"
      aria-labelledby={id}
      initial={{ opacity: 0, y: 14 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, ease: EASE_OUT, delay }}
    >
      <header className="mb-6 flex items-start gap-4">
        <span className="mt-1 grid h-11 w-11 shrink-0 place-items-center rounded-full border border-ember-500/25 bg-ember-500/10 text-ember-400">
          <Icon size={20} strokeWidth={1.75} aria-hidden="true" />
        </span>
        <div>
          <h2 id={id} className="font-display text-3xl italic text-ink" style={{ fontVariationSettings: '"opsz" 36' }}>
            {title}
          </h2>
          <p className="mt-1 max-w-[70ch] text-[15px] leading-6 text-ink-mute">{hint}</p>
        </div>
      </header>
      {children}
    </motion.section>
  )
}

/** One thing Echo saves: what it is, a switch, and a line that says what the switch is doing right now. */
function SaveTile({
  icon: Icon,
  title,
  does,
  status,
  active,
  control,
}: {
  icon: LucideIcon
  title: string
  does: string
  status: string
  active: boolean
  control: ReactNode
}) {
  return (
    <div className="card flex flex-col p-5">
      <div className="flex items-center justify-between gap-3">
        <div className="flex min-w-0 items-center gap-3">
          <span className="grid h-10 w-10 shrink-0 place-items-center rounded-full border border-ember-500/25 bg-ember-500/10 text-ember-400">
            <Icon size={18} strokeWidth={1.75} aria-hidden="true" />
          </span>
          <h3 className="truncate text-lg font-medium text-ink">{title}</h3>
        </div>
        {control}
      </div>
      <p className="mt-3 text-[15px] leading-6 text-ink-mute">{does}</p>
      <p className={`mt-auto flex items-center gap-2.5 pt-4 text-[15px] ${active ? 'text-ember-300' : 'text-ink-faint'}`}>
        <span
          aria-hidden="true"
          className={`h-2 w-2 shrink-0 rounded-full ${active ? 'bg-ember-400 shadow-[0_0_8px_rgba(245,169,71,0.6)]' : 'bg-white/[0.18]'}`}
        />
        {status}
      </p>
    </div>
  )
}

/** One "leave out" list: a text box, an add button and removable chips. Saves on every change. */
function KeepOutCard({
  icon: Icon,
  title,
  help,
  placeholder,
  entries,
  check,
  onSave,
}: {
  icon: LucideIcon
  title: string
  help: string
  placeholder: string
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
      setMessage('Couldn’t save just now. The list is as it was. Try again in a moment.')
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

  const inputId = `keep-out-${title}`
  return (
    <div className="card p-6 sm:p-7">
      <div className="flex items-center gap-3">
        <span className="grid h-10 w-10 shrink-0 place-items-center rounded-full border border-ember-500/25 bg-ember-500/10 text-ember-400">
          <Icon size={18} strokeWidth={1.75} aria-hidden="true" />
        </span>
        <label htmlFor={inputId} className="text-lg font-medium text-ink">
          {title}
        </label>
      </div>
      <p className="mt-4 text-[15px] leading-6 text-ink-mute md:min-h-[4.5rem]">{help}</p>
      <div className="mt-4 flex gap-2">
        <input
          id={inputId}
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
          className="input-study min-w-0 flex-1 px-4 py-2.5 text-[15px]"
        />
        <button type="button" onClick={() => void add()} disabled={busy} className="btn-ghost px-5 py-2.5 text-sm disabled:opacity-50">
          {busy ? 'saving…' : 'add'}
        </button>
      </div>
      {message && (
        <p role="alert" className="mt-3 text-[15px] leading-6 text-ink-soft">
          {message}
        </p>
      )}
      {entries.length > 0 ? (
        <ul className="mt-4 flex flex-wrap gap-2" aria-label={`${title} left out`}>
          {entries.map((entry) => (
            <li
              key={entry}
              className="inline-flex items-center gap-2 rounded-pill border border-white/[0.1] bg-night-800 py-1.5 pl-4 pr-2 font-mono text-sm text-ink-soft"
            >
              {entry}
              <button
                type="button"
                onClick={() => void save(entries.filter((other) => other !== entry))}
                disabled={busy}
                aria-label={`Stop leaving out ${entry}`}
                className="rounded-full p-1 text-ink-mute transition hover:text-ink disabled:opacity-50"
              >
                <X size={13} aria-hidden="true" />
              </button>
            </li>
          ))}
        </ul>
      ) : (
        <p className="mt-4 text-[15px] text-ink-faint">Nothing left out yet.</p>
      )}
    </div>
  )
}

/** The lines Echo keeps to, each with a tick so the list reads as a promise. */
const PROMISES = [
  'Incognito is never tracked.',
  'Nothing you type is captured.',
  'App content (Slack, Notion, Jira) is never read.',
  'Only your question and short snippets ever reach an LLM.',
  'Everything else lives and dies on this machine.',
] as const

export default function SettingsPage() {
  const [settings, setSettings] = useState<LocalSettings>(loadSettings)
  const [retention, setRetention] = useState<RetentionChoice>('forever')
  const [excluded, setExcluded] = useState<ExclusionLists>({ excluded_domains: [], excluded_senders: [] })
  const [exporting, setExporting] = useState<'json' | 'csv' | null>(null)
  const [exportMessage, setExportMessage] = useState<string | null>(null)
  const [demo, setDemo] = useState(isDemoMode())
  const [reminderStatus, setReminderStatus] = useState<ReminderStatus | null>(null)

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

  // The reminders switch also starts from the backend flag, and its numbers fill the card.
  useEffect(() => {
    let cancelled = false
    void fetchReminderStatus().then((status) => {
      if (cancelled) return
      setReminderStatus(status)
      if (!isDemoMode() && status.available) setSettings((current) => ({ ...current, regretReminders: status.enabled }))
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
      const backendKey = CAPTURE_KEY_MAP[key as SourceKey]
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
      <PageTitle eyebrow="settings" title="The house rules." />
      <motion.p
        className="mt-6 max-w-[62ch] text-[17px] leading-8 text-ink-mute"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.6, delay: 0.3 }}
      >
        What Echo saves, what it leaves out, how long it remembers and when it speaks up. Every change is saved the
        moment you make it.
      </motion.p>

      <Section
        icon={Archive}
        title="What Echo saves"
        hint="Switch a source off and Echo stops saving new things from it. Whatever is already saved stays."
        delay={0.1}
      >
        <div className="grid grid-cols-1 gap-5 md:grid-cols-3">
          {SOURCES.map((source) => {
            const on = settings[source.key]
            return (
              <SaveTile
                key={source.key}
                icon={source.icon}
                title={source.title}
                does={source.does}
                status={on ? source.on : source.off}
                active={on}
                control={<Switch checked={on} onChange={(value) => update(source.key, value)} label={`Save ${source.title}`} />}
              />
            )
          })}
        </div>
      </Section>

      <Section
        icon={Ban}
        title="What Echo leaves out"
        hint="Echo skips these completely: nothing from them is saved or searchable. It applies to new activity; what is already saved stays until you delete it on the Timeline."
        delay={0.15}
      >
        <div className="grid grid-cols-1 gap-5 md:grid-cols-2">
          <KeepOutCard
            icon={Globe}
            title="Sites"
            help="Type a site, like example.com. That site and its subsites, such as mail.example.com, are skipped."
            placeholder="example.com"
            entries={excluded.excluded_domains}
            check={checkSite}
            onSave={(next) => saveList('excluded_domains', next)}
          />
          <KeepOutCard
            icon={Mail}
            title="Senders"
            help="Type part of an email address, like newsletter@. Every email whose sender contains it is skipped."
            placeholder="newsletter@"
            entries={excluded.excluded_senders}
            check={checkSender}
            onSave={(next) => saveList('excluded_senders', next)}
          />
        </div>
      </Section>

      <Section icon={Hourglass} title="How long Echo remembers" hint="Choose when old memories are deleted." delay={0.2}>
        <div role="radiogroup" aria-label="How long Echo keeps your memories" className="grid grid-cols-1 gap-5 md:grid-cols-3">
          {RETENTION_OPTIONS.map((option) => {
            const chosen = retention === option.value
            return (
              <button
                key={option.value}
                type="button"
                role="radio"
                aria-checked={chosen}
                onClick={() => chooseRetention(option.value)}
                className={`relative rounded-card border p-6 text-left transition duration-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember-400/60 sm:p-7 ${
                  chosen
                    ? 'border-ember-500/60 bg-ember-500/[0.07]'
                    : 'border-white/[0.07] bg-night-850 hover:-translate-y-0.5 hover:border-ember-500/35'
                }`}
              >
                <span className="flex items-center justify-between gap-3">
                  <span className="text-lg font-medium text-ink">{option.label}</span>
                  <span
                    aria-hidden="true"
                    className={`grid h-6 w-6 place-items-center rounded-full border ${
                      chosen ? 'border-ember-400 bg-ember-500 text-night-950' : 'border-white/[0.18]'
                    }`}
                  >
                    {chosen && <Check size={14} strokeWidth={3} />}
                  </span>
                </span>
                <span className="mt-3 block text-[15px] leading-6 text-ink-mute">{option.detail}</span>
              </button>
            )
          })}
        </div>
        {retention !== 'forever' && (
          <p className="mt-5 max-w-[70ch] text-[15px] leading-6 text-ink-soft">
            Deleting happens the next time Echo’s backend starts. It removes the saved text, its search entry and its
            cache, and it can’t be undone.
          </p>
        )}
      </Section>

      <Section
        icon={Bell}
        title="Reminders"
        hint="A small note in the corner of the Echo page, in your own words. It never covers or blocks anything."
        delay={0.25}
      >
        <ReminderPanel
          status={reminderStatus}
          enabled={settings.regretReminders}
          onChange={(value) => update('regretReminders', value)}
        />
      </Section>

      <Section
        icon={ShieldCheck}
        title="Your data"
        hint="A copy is yours whenever you want it, and these are the rules Echo keeps."
        delay={0.3}
      >
        <div className="grid grid-cols-1 gap-5 md:grid-cols-2">
          <div className="card flex flex-col p-6 sm:p-7">
            <h3 className="text-lg font-medium text-ink">Take a copy with you</h3>
            <p className="mt-3 text-[15px] leading-6 text-ink-mute">
              Everything Echo saved about your items: title, source, times, time spent, and the sender, link or channel.
              The full text, the search index and your Google sign-in are not included.
            </p>
            <div className="mt-auto flex flex-wrap gap-3 pt-6">
              {(['json', 'csv'] as const).map((format) => (
                <button
                  key={format}
                  type="button"
                  onClick={() => void exportData(format)}
                  disabled={exporting !== null}
                  className="btn-ghost inline-flex items-center gap-2 px-5 py-2.5 text-sm disabled:opacity-50"
                >
                  <Download size={15} aria-hidden="true" /> {exporting === format ? 'preparing…' : `download ${format.toUpperCase()}`}
                </button>
              ))}
            </div>
            {exportMessage && (
              <p role="status" className="mt-4 text-[15px] leading-6 text-ink-soft">
                {exportMessage}
              </p>
            )}
          </div>

          <div className="card border-ember-500/15 p-6 sm:p-7">
            <h3 className="text-lg font-medium text-ink">The promise</h3>
            <ul className="mt-4 space-y-3">
              {PROMISES.map((line) => (
                <li key={line} className="flex items-start gap-3 text-[15px] leading-6 text-ink-soft">
                  <Check size={16} strokeWidth={2.5} aria-hidden="true" className="mt-1 shrink-0 text-ember-400" />
                  {line}
                </li>
              ))}
            </ul>
          </div>
        </div>
      </Section>

      {/* The technical detail comes last, out of the way at the bottom right */}
      <div className="mt-14 flex justify-end">
        <div className="inline-flex flex-wrap items-center gap-x-4 gap-y-1 rounded-pill border border-white/[0.08] bg-night-850 px-5 py-2.5">
          <span className="flex items-center gap-2.5 text-sm text-ink-soft">
            <span aria-hidden="true" className={`h-2 w-2 rounded-full ${demo ? 'bg-ember-400' : 'bg-moss shadow-[0_0_8px_rgba(157,191,142,0.6)]'}`} />
            {demo ? 'Offline. Showing demo data.' : 'Connected to Echo’s backend'}
          </span>
          <span className="font-mono text-xs text-ink-faint">{apiBase}</span>
        </div>
      </div>
    </div>
  )
}
