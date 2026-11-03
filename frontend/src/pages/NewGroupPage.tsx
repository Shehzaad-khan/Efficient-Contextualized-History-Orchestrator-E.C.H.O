import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import { ArrowLeft, ArrowRight, Check, Layers, Lightbulb, Plus, Search, Target, X } from 'lucide-react'
import AddRuleForm from '../components/AddRuleForm'
import ScrollList from '../components/ScrollList'
import SourceBadge from '../components/SourceBadge'
import { ApiError, createGroupWithItems, fetchRecent } from '../lib/api'
import { asUtc, formatWhen } from '../lib/format'
import { filterItems, itemLocation } from '../lib/itemFilter'
import { RULE_KINDS, ruleKey, ruleSentence } from '../lib/rules'
import type { MemoryItem, RuleType } from '../lib/types'

const EASE_OUT = [0.22, 1, 0.36, 1] as const

// Examples a group needs before Echo starts suggesting items (backend wba/group_manager.py MIN_CONFIRMED_FOR_KNN).
const EXAMPLES_NEEDED = 5
// The most the recent-items route gives (backend/wellbeing.py `/recent`, le=100).
const PICKER_ITEMS = 100
const NAME_EXAMPLES = ['Placement Prep', 'Exam revision', 'Side project']
// A hint made from a shared mail provider would match half the inbox.
const SHARED_MAIL_PROVIDERS = new Set(['gmail.com', 'googlemail.com', 'outlook.com', 'hotmail.com', 'yahoo.com', 'icloud.com'])
const MAX_SUGGESTED_HINTS = 6

const STEPS = ['Name it', 'Pick examples', 'Add hints', 'Review'] as const

interface Hint {
  type: RuleType
  value: string
}

interface SuggestedHint extends Hint {
  count: number
}

function mailDomain(sender: string | null | undefined): string | null {
  const match = /[\w.+'-]+@([\w-]+(?:\.[\w-]+)+)/.exec(sender ?? '')
  return match ? match[1].toLowerCase() : null
}

/** Sites, mail senders and channels that two or more of the picked items share: ready-made hints. */
function suggestHints(items: MemoryItem[]): SuggestedHint[] {
  const counts = new Map<string, SuggestedHint>()
  function tally(type: RuleType, value: string | null | undefined) {
    const clean = value?.trim()
    if (!clean) return
    const key = ruleKey(type, clean)
    const found = counts.get(key)
    if (found) found.count += 1
    else counts.set(key, { type, value: type === 'domain' ? clean.replace(/^www\./i, '') : clean, count: 1 })
  }
  for (const item of items) {
    if (item.source_type === 'chrome') tally('domain', item.domain)
    else if (item.source_type === 'youtube') tally('channel', item.channel_name)
    else {
      const domain = mailDomain(item.sender)
      if (domain && !SHARED_MAIL_PROVIDERS.has(domain)) tally('sender', domain)
    }
  }
  return [...counts.values()]
    .filter((hint) => hint.count >= 2)
    .sort((a, b) => b.count - a.count)
    .slice(0, MAX_SUGGESTED_HINTS)
}

function createErrorText(error: unknown): string {
  if (error instanceof ApiError && error.status === 400 && error.detail) return error.detail
  return 'Couldn’t create the group, so nothing was saved. Try again in a moment.'
}

function StepIndicator({ current }: { current: number }) {
  return (
    <ol className="flex items-center" aria-label="Steps">
      {STEPS.map((label, index) => {
        const done = index < current
        const active = index === current
        return (
          <li key={label} className="flex items-center" aria-current={active ? 'step' : undefined}>
            <span className="flex items-center gap-2.5">
              <span
                className={`flex h-9 w-9 items-center justify-center rounded-full border font-mono text-sm transition-colors duration-200 [@media(max-height:780px)]:h-7 [@media(max-height:780px)]:w-7 ${
                  active
                    ? 'border-ember-500 bg-ember-500 text-night-950 shadow-[0_0_18px_rgba(245,169,71,0.35)]'
                    : done
                      ? 'border-ember-500/50 bg-ember-500/10 text-ember-400'
                      : 'border-white/10 text-ink-faint'
                }`}
              >
                {done ? <Check size={16} aria-hidden="true" /> : index + 1}
              </span>
              <span
                className={`text-[15px] font-medium ${active ? 'text-ink' : 'hidden sm:inline'} ${
                  done ? 'text-ink-soft' : active ? '' : 'text-ink-faint'
                }`}
              >
                {label}
              </span>
            </span>
            {index < STEPS.length - 1 && <span aria-hidden="true" className="mx-3 h-px w-4 bg-white/10 sm:w-8" />}
          </li>
        )
      })}
    </ol>
  )
}

function StepHeading({ title, children, hideOnShort = false }: { title: string; children: ReactNode; hideOnShort?: boolean }) {
  return (
    <div>
      <h2 className="font-display text-4xl italic text-ink" style={{ fontVariationSettings: '"opsz" 48, "SOFT" 40' }}>
        {title}
      </h2>
      <p className={`mt-2 max-w-[64ch] text-[17px] leading-8 text-ink-mute ${hideOnShort ? '[@media(max-height:780px)]:hidden' : ''}`}>{children}</p>
    </div>
  )
}

/** The small caption above a field or a group of chips. */
function FieldLabel({ children, htmlFor }: { children: ReactNode; htmlFor?: string }) {
  return (
    <label htmlFor={htmlFor} className="meta mb-2.5 block text-[13px]">
      {children}
    </label>
  )
}

const PRIMARY_BUTTON =
  'inline-flex items-center gap-2.5 rounded-pill bg-ember-500 px-7 py-3 [@media(max-height:780px)]:py-2 text-base font-semibold text-night-950 transition duration-200 ease-out hover:bg-ember-400 active:scale-[0.97] disabled:cursor-not-allowed disabled:bg-white/[0.06] disabled:text-ink-faint disabled:hover:bg-white/[0.06] disabled:active:scale-100'
const GHOST_BUTTON = 'btn-ghost inline-flex items-center gap-2 px-6 py-3 [@media(max-height:780px)]:py-2 text-base disabled:opacity-40'

/** Three plain statements of how the flow ends up helping, shown on the first step. */
const HOW_IT_WORKS: ReadonlyArray<{ icon: typeof Target; title: string; text: string }> = [
  { icon: Layers, title: `Pick ${EXAMPLES_NEEDED} examples`, text: 'Things you have already seen that belong to this goal.' },
  { icon: Lightbulb, title: 'Add hints', text: 'Optional: a website, a sender or a channel to look for.' },
  { icon: Target, title: 'Echo suggests more', text: 'In your Sunday review. Nothing joins without your yes.' },
]

/**
 * The guided "new goal" flow. A group does not exist until the last step: the name, the examples
 * and the hints live only in this page, and one call at the end creates all of it or nothing.
 * Leaving earlier creates nothing.
 *
 * The box is a fixed part of the window with its steps on top and its buttons at the bottom, so the
 * only thing that ever scrolls is the middle: the list of items on the second step, or the step's own
 * content on the others. The wheel over any part of the box moves that one scroller.
 */
export default function NewGroupPage() {
  const navigate = useNavigate()
  const [step, setStep] = useState(0)
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [picked, setPicked] = useState<string[]>([])
  const [hints, setHints] = useState<Hint[]>([])
  const [recent, setRecent] = useState<MemoryItem[] | null>(null)
  const [search, setSearch] = useState('')
  const [creating, setCreating] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [leaving, setLeaving] = useState(false)
  const panelRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    let cancelled = false
    void fetchRecent(PICKER_ITEMS).then((data) => !cancelled && setRecent(data))
    return () => {
      cancelled = true
    }
  }, [])

  // On the list step the wheel anywhere on the page scrolls the list (marked `data-scroll-target`) while it has
  // room. Over the list itself the browser does it unaided.
  useEffect(() => {
    const panel = panelRef.current
    if (!panel) return
    function forwardWheel(event: WheelEvent) {
      const scroller = panel?.querySelector<HTMLElement>('[data-scroll-target]')
      if (!scroller || scroller.contains(event.target as Node)) return
      // Only while the list still has room in that direction; at its end the page gets the wheel again.
      const roomBelow = scroller.scrollTop + scroller.clientHeight < scroller.scrollHeight - 1
      const roomAbove = scroller.scrollTop > 0
      if (event.deltaY > 0 ? !roomBelow : !roomAbove) return
      scroller.scrollTop += event.deltaY
      event.preventDefault()
      event.stopPropagation() // the page's smooth scrolling listens on the window; it must not also move
    }
    panel.addEventListener('wheel', forwardWheel, { passive: false })
    return () => panel.removeEventListener('wheel', forwardWheel)
  }, [])

  const pickedSet = useMemo(() => new Set(picked), [picked])
  const pickedItems = useMemo(
    () => picked.flatMap((id) => recent?.find((item) => item.memory_id === id) ?? []),
    [picked, recent],
  )
  const candidates = useMemo(() => filterItems(recent ?? [], search), [recent, search])
  const suggestions = useMemo(() => suggestHints(pickedItems), [pickedItems])
  const hintKeys = useMemo(() => new Set(hints.map((hint) => ruleKey(hint.type, hint.value))), [hints])

  const trimmedName = name.trim()
  const remaining = Math.max(0, EXAMPLES_NEEDED - picked.length)
  const canContinue = step === 0 ? trimmedName !== '' : step === 1 ? remaining === 0 : true
  const hasDraft = trimmedName !== '' || picked.length > 0 || hints.length > 0

  function togglePick(memoryId: string) {
    setPicked((current) => (current.includes(memoryId) ? current.filter((id) => id !== memoryId) : [...current, memoryId]))
  }

  function addHint(type: RuleType, value: string) {
    setHints((current) => (current.some((hint) => ruleKey(hint.type, hint.value) === ruleKey(type, value)) ? current : [...current, { type, value: value.trim() }]))
  }

  async function addHintFromForm(type: RuleType, value: string): Promise<void> {
    if (hintKeys.has(ruleKey(type, value))) throw new ApiError(400, 'That hint is already in the list.', '400')
    addHint(type, value)
  }

  function removeHint(target: Hint) {
    setHints((current) => current.filter((hint) => ruleKey(hint.type, hint.value) !== ruleKey(target.type, target.value)))
  }

  async function create() {
    if (creating || remaining > 0 || trimmedName === '') return
    setCreating(true)
    setError(null)
    try {
      const created = await createGroupWithItems({
        group_name: trimmedName,
        description: description.trim() || null,
        memory_ids: picked,
        rules: hints.map((hint) => ({ rule_type: hint.type, rule_value: hint.value })),
      })
      navigate('/groups', {
        state: { createdGroup: { name: created.group_name, items: created.member_count, hints: hints.length } },
      })
    } catch (failure) {
      setError(createErrorText(failure))
      setCreating(false)
    }
  }

  function leave() {
    if (hasDraft && !leaving) {
      setLeaving(true)
      return
    }
    navigate('/groups')
  }

  const nextLabel =
    step === 0
      ? 'Next: pick examples'
      : step === 1
        ? remaining > 0
          ? `Pick ${remaining} more`
          : 'Next: add hints'
        : hints.length === 0
          ? 'Skip hints'
          : 'Next: review'

  return (
    <div
      className={`mx-auto w-full max-w-3xl ${
        step === 1 ? 'flex h-[calc(100dvh-10.5rem)] min-h-[26rem] flex-col lg:h-[calc(100dvh-6rem)]' : ''
      }`}
    >

      {leaving && (
        <div role="alert" className="mt-5 rounded-card border border-youtube/30 bg-night-900/60 px-5 py-4">
          <p className="text-base text-ink-soft">Leave without creating it? Nothing has been saved yet, so the draft is lost.</p>
          <div className="mt-3 flex gap-3">
            <button
              type="button"
              onClick={() => navigate('/groups')}
              className="btn-ghost border-youtube/40 px-4 py-2 text-sm normal-case text-youtube hover:border-youtube/70"
            >
              Yes, leave
            </button>
            <button type="button" onClick={() => setLeaving(false)} className="btn-ghost px-4 py-2 text-sm normal-case">
              Keep going
            </button>
          </div>
        </div>
      )}

      {/* Open on the page, no box: the steps and a progress line on top, the step below, and a floating action
          bar that stays in reach. Only the list on the second step scrolls inside itself. */}
      <div ref={panelRef} className={`relative mt-1 ${step === 1 ? 'flex min-h-0 flex-1 flex-col' : ''}`}>
        <span aria-hidden="true" className="pointer-events-none absolute -left-24 top-28 h-64 w-64 rounded-full bg-ember-500/10 blur-3xl" />

        <div className="relative">
          <div className="flex items-center justify-between gap-4">
            <StepIndicator current={step} />
            <button
              type="button"
              onClick={leave}
              className="meta inline-flex shrink-0 items-center gap-1.5 text-[13px] text-ink-mute transition hover:text-ink-soft"
            >
              <ArrowLeft size={14} aria-hidden="true" /> groups
            </button>
          </div>
          <div className="mt-4 [@media(max-height:780px)]:mt-2.5 h-1 rounded-full bg-night-700/60" aria-hidden="true">
            <motion.div
              className="h-full rounded-full bg-gradient-to-r from-ember-600 to-ember-400"
              animate={{ width: `${((step + 1) / STEPS.length) * 100}%` }}
              transition={{ duration: 0.4, ease: EASE_OUT }}
            />
          </div>
        </div>

        <AnimatePresence mode="wait">
          <motion.section
            key={step}
            className={`relative ${step === 1 ? 'mt-6 [@media(max-height:780px)]:mt-3 flex min-h-0 flex-1 flex-col' : 'mt-9'}`}
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -6, transition: { duration: 0.12 } }}
            transition={{ duration: 0.25, ease: EASE_OUT }}
            aria-label={STEPS[step]}
          >
            {step === 0 && (
              <div>
                <StepHeading title="What are you working towards?">
                  A goal group gathers what belongs to one goal, and Echo learns from your examples.
                </StepHeading>

                <div className="mt-6">
                  <FieldLabel htmlFor="new-group-name">name</FieldLabel>
                  <div className="flex items-center gap-3 rounded-pill border border-ember-500/30 bg-night-900 px-5 py-1.5 transition duration-200 focus-within:border-ember-500/60 focus-within:shadow-[0_0_0_3px_rgba(245,169,71,0.14)]">
                    <Target size={20} className="shrink-0 text-ember-400" aria-hidden="true" />
                    <input
                      id="new-group-name"
                      value={name}
                      onChange={(event) => setName(event.target.value)}
                      onKeyDown={(event) => {
                        if (event.key === 'Enter' && canContinue) setStep(1)
                      }}
                      maxLength={100}
                      autoFocus
                      placeholder="e.g. Placement Prep"
                      className="min-w-0 flex-1 bg-transparent py-2.5 text-lg text-ink placeholder:text-base placeholder:text-ink-faint focus:outline-none"
                    />
                  </div>
                  {trimmedName === '' && (
                    <div className="mt-3.5 flex flex-wrap items-center gap-2.5">
                      <span className="text-[15px] text-ink-faint">Try</span>
                      {NAME_EXAMPLES.map((example) => (
                        <button
                          key={example}
                          type="button"
                          onClick={() => setName(example)}
                          className="rounded-pill border border-white/10 px-4 py-1.5 text-[15px] text-ink-soft transition hover:border-ember-500/40 hover:text-ink"
                        >
                          {example}
                        </button>
                      ))}
                    </div>
                  )}
                </div>

                <div className="mt-6">
                  <FieldLabel htmlFor="new-group-description">one line about it · optional</FieldLabel>
                  <input
                    id="new-group-description"
                    value={description}
                    onChange={(event) => setDescription(event.target.value)}
                    maxLength={500}
                    placeholder="e.g. everything for the campus drive in January"
                    className="input-study px-5 py-2.5 text-base"
                  />
                </div>

                <ol aria-label="How it works" className="mt-6 space-y-2.5 rounded-card border border-white/[0.06] bg-night-900/50 p-4">
                  {HOW_IT_WORKS.map(({ icon: Icon, title, text }) => (
                    <li key={title} className="flex items-start gap-3.5">
                      <span className="mt-0.5 grid h-8 w-8 shrink-0 place-items-center rounded-full border border-ember-500/30 bg-ember-500/10 text-ember-400">
                        <Icon size={15} strokeWidth={1.75} aria-hidden="true" />
                      </span>
                      <span className="text-[15px] leading-6 text-ink-mute">
                        <span className="font-medium text-ink">{title}.</span> {text}
                      </span>
                    </li>
                  ))}
                </ol>
              </div>
            )}

            {step === 1 && (
              <div className="flex min-h-0 flex-1 flex-col">
                <div className="flex shrink-0 flex-wrap items-baseline justify-between gap-x-6 gap-y-1" aria-live="polite">
                <h2 className="font-display text-4xl italic text-ink [@media(max-height:780px)]:text-2xl" style={{ fontVariationSettings: '"opsz" 48, "SOFT" 40' }}>
                  Pick {EXAMPLES_NEEDED} examples.
                </h2>
                <p className="text-base">
                  <span className="font-mono text-lg text-ink">{picked.length}</span>
                  <span className="text-ink-mute"> of {EXAMPLES_NEEDED} picked · </span>
                  <span className={remaining === 0 ? 'text-moss' : 'text-ember-300'}>
                    {remaining === 0
                      ? picked.length === EXAMPLES_NEEDED
                        ? 'enough to start, add more if you like'
                        : 'enough to start'
                      : `${remaining} more to go`}
                  </span>
                </p>
              </div>
              <div className="mt-3 flex shrink-0 gap-2 [@media(max-height:780px)]:hidden" aria-hidden="true">
                {Array.from({ length: EXAMPLES_NEEDED }, (_, index) => (
                  <span
                    key={index}
                    className={`h-2 flex-1 rounded-full transition-colors duration-300 ${
                      index < picked.length ? 'bg-gradient-to-r from-ember-600 to-ember-400' : 'bg-night-700/60'
                    }`}
                  />
                ))}
              </div>
              <p className="mt-3 shrink-0 text-[15px] text-ink-mute [@media(max-height:780px)]:hidden">
                Things you have already seen that belong to “{trimmedName}”.
              </p>

              <label className="input-study mt-3 flex shrink-0 items-center gap-3 px-4 py-2.5 [@media(max-height:780px)]:mt-2 [@media(max-height:780px)]:py-1.5">
                  <Search size={17} aria-hidden="true" className="shrink-0 text-ink-faint" />
                  <input
                    value={search}
                    onChange={(event) => setSearch(event.target.value)}
                    placeholder={`Filter your ${PICKER_ITEMS} most recent items`}
                    aria-label="Filter recent items"
                    className="flex-1 bg-transparent text-base text-ink placeholder:text-ink-faint focus:outline-none"
                  />
                </label>

                {recent === null ? (
                  <div className="mt-4 space-y-3" role="status" aria-label="Loading recent items">
                    {[0, 1, 2, 3].map((index) => (
                      <div key={index} className="h-14 animate-pulse rounded bg-night-800/60" />
                    ))}
                  </div>
                ) : candidates.length === 0 ? (
                  <p className="mt-5 text-base text-ink-mute">
                    {recent.length === 0
                      ? 'Nothing captured yet. Browse for a while, then come back.'
                      : 'Nothing in your recent items matches that.'}
                  </p>
                ) : (
                  <ScrollList
                    label="Recent items you can pick"
                    className="mt-4 min-h-0 flex-1 rounded-card border border-white/[0.07] bg-night-850/60 px-3 py-1"
                    listClassName="h-full"
                  >
                    {candidates.map((item) => {
                      const isPicked = pickedSet.has(item.memory_id)
                      return (
                        <li key={item.memory_id} className="border-b border-white/[0.05] last:border-0">
                          <button
                            type="button"
                            role="checkbox"
                            aria-checked={isPicked}
                            onClick={() => togglePick(item.memory_id)}
                            className={`flex w-full items-center gap-4 rounded-sm px-1 py-3 [@media(max-height:780px)]:py-2 text-left transition hover:bg-white/[0.03] focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ember-500/40 ${
                              isPicked ? 'bg-ember-500/[0.05]' : ''
                            }`}
                          >
                            <span
                              aria-hidden="true"
                              className={`flex h-6 w-6 shrink-0 items-center justify-center rounded-[7px] border transition-colors duration-150 ${
                                isPicked ? 'border-ember-500 bg-ember-500 text-night-950' : 'border-white/25 text-transparent'
                              }`}
                            >
                              <Check size={15} strokeWidth={3} />
                            </span>
                            <span className="w-24 shrink-0">
                              <SourceBadge source={item.source_type} />
                            </span>
                            <span className={`min-w-0 flex-1 truncate text-base ${isPicked ? 'text-ink' : 'text-ink-soft'}`}>
                              {item.title || '(untitled)'}
                              {itemLocation(item) && <span className="text-sm text-ink-mute"> · {itemLocation(item)}</span>}
                            </span>
                            <span className="hidden shrink-0 text-[13px] text-ink-faint sm:block">
                              {formatWhen(asUtc(item.last_accessed_at ?? item.created_at))}
                            </span>
                          </button>
                        </li>
                      )
                    })}
                  </ScrollList>
                )}
                </div>
            )}

            {step === 2 && (
              <div>
                <StepHeading title="Give Echo some hints.">
                  Hints tell Echo what else to look for. They are optional: skip this if you are not sure, and add or
                  change them any time from the group’s page.
                </StepHeading>

                {suggestions.length > 0 && (
                  <div className="mt-7">
                    <FieldLabel>from your {picked.length} examples · tap to add</FieldLabel>
                    <ul className="flex flex-wrap gap-2.5">
                      {suggestions.map((suggestion) => {
                        const on = hintKeys.has(ruleKey(suggestion.type, suggestion.value))
                        return (
                          <li key={ruleKey(suggestion.type, suggestion.value)}>
                            <button
                              type="button"
                              aria-pressed={on}
                              onClick={() => (on ? removeHint(suggestion) : addHint(suggestion.type, suggestion.value))}
                              className={`inline-flex items-center gap-2.5 rounded-pill border px-4 py-2 text-left text-base transition duration-200 ${
                                on
                                  ? 'border-ember-500/60 bg-ember-500/15 text-ember-300'
                                  : 'border-white/10 text-ink-soft hover:border-ember-500/40'
                              }`}
                            >
                              {on ? <Check size={16} aria-hidden="true" /> : <Plus size={16} aria-hidden="true" />}
                              <span>{ruleSentence(suggestion.type, suggestion.value)}</span>
                              <span className="text-sm text-ink-faint">{suggestion.count} of your picks</span>
                            </button>
                          </li>
                        )
                      })}
                    </ul>
                  </div>
                )}

                <div className="mt-7">
                  <FieldLabel>or add your own</FieldLabel>
                  <AddRuleForm onAdd={addHintFromForm} />
                </div>

                <div className="mt-8">
                  <FieldLabel>
                    {hints.length === 0 ? 'no hints yet · that is fine' : `${hints.length} ${hints.length === 1 ? 'hint' : 'hints'} so far`}
                  </FieldLabel>
                  {hints.length > 0 && (
                    <ul>
                      {hints.map((hint) => (
                        <li
                          key={ruleKey(hint.type, hint.value)}
                          className="flex items-center justify-between gap-4 border-b border-white/[0.05] py-3 last:border-0"
                        >
                          <span className="min-w-0 text-base text-ink-soft">
                            <span className="mr-3 text-sm text-ink-faint">{RULE_KINDS[hint.type].short}</span>
                            <span className="break-words font-mono text-ink">{hint.type === 'time_window' ? hint.value.replace(',', ' → ') : hint.value}</span>
                          </span>
                          <button
                            type="button"
                            onClick={() => removeHint(hint)}
                            aria-label={`Remove hint: ${ruleSentence(hint.type, hint.value)}`}
                            className="shrink-0 rounded-full p-2 text-ink-faint transition hover:text-youtube"
                          >
                            <X size={17} aria-hidden="true" />
                          </button>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              </div>
            )}

            {step === 3 && (
              <div>
                <StepHeading title="Ready to start?">
                  This is the moment “{trimmedName}” becomes a group. Until you press create, nothing has been saved.
                </StepHeading>

                <dl className="mt-7 space-y-6">
                  <div className="rounded-card border border-white/[0.06] bg-night-900/50 p-5">
                    <dt className="meta text-[13px]">goal</dt>
                    <dd className="mt-1.5 text-xl font-medium text-ink">{trimmedName}</dd>
                    {description.trim() && <dd className="mt-1 text-base text-ink-mute">{description.trim()}</dd>}
                  </div>
                  <div className="rounded-card border border-white/[0.06] bg-night-900/50 p-5">
                    <dt className="meta text-[13px]">
                      {picked.length} {picked.length === 1 ? 'example' : 'examples'}
                    </dt>
                    <dd>
                      <ul className="mt-2.5 space-y-2.5">
                        {pickedItems.map((item) => (
                          <li key={item.memory_id} className="flex items-center gap-3.5">
                            <SourceBadge source={item.source_type} />
                            <span className="min-w-0 flex-1 truncate text-base text-ink-soft">{item.title || '(untitled)'}</span>
                          </li>
                        ))}
                      </ul>
                    </dd>
                  </div>
                  <div className="rounded-card border border-white/[0.06] bg-night-900/50 p-5">
                    <dt className="meta text-[13px]">hints</dt>
                    <dd className="mt-2 text-base text-ink-soft">
                      {hints.length === 0 ? (
                        <span className="text-ink-mute">None. Echo will learn from your examples alone.</span>
                      ) : (
                        <ul className="space-y-1.5">
                          {hints.map((hint) => (
                            <li key={ruleKey(hint.type, hint.value)}>Looks for {ruleSentence(hint.type, hint.value)}</li>
                          ))}
                        </ul>
                      )}
                    </dd>
                  </div>
                </dl>

                <p className="mt-6 text-[15px] leading-7 text-ink-mute">
                  <span className="font-medium text-ink-soft">What happens next: </span>
                  Echo starts suggesting similar items right away. You will see them in the Sunday review on Groups, and
                  nothing is added without your yes, except very sure matches, which you can undo.
                </p>

                {error && (
                  <p role="alert" className="mt-5 text-base text-youtube">
                    {error}
                  </p>
                )}
              </div>
            )}
          </motion.section>
        </AnimatePresence>

        <div className="sticky bottom-24 z-10 mt-4 flex shrink-0 flex-wrap items-center justify-between gap-3 rounded-pill border border-white/[0.09] bg-night-950/85 px-3 py-2 [@media(max-height:780px)]:py-1.5 shadow-lamp backdrop-blur-study sm:px-4 lg:bottom-6">
          {step > 0 ? (
            <button
              type="button"
              onClick={() => {
                setStep(step - 1)
                setError(null)
              }}
              disabled={creating}
              className={GHOST_BUTTON}
            >
              <ArrowLeft size={16} aria-hidden="true" /> Back
            </button>
          ) : (
            <Link
              to="/groups"
              className="px-2 py-3 text-base text-ink-mute transition hover:text-ink-soft"
              onClick={(event) => {
                if (hasDraft) {
                  event.preventDefault()
                  setLeaving(true)
                }
              }}
            >
              Cancel
            </Link>
          )}

          {step < STEPS.length - 1 ? (
            <div className="flex items-center gap-5">
              {step === 2 && hints.length === 0 && <span className="hidden text-[15px] text-ink-faint sm:inline">Skipping is fine</span>}
              <button type="button" onClick={() => setStep(step + 1)} disabled={!canContinue} className={PRIMARY_BUTTON}>
                {nextLabel}
                <ArrowRight size={16} aria-hidden="true" />
              </button>
            </div>
          ) : (
            <button type="button" onClick={() => void create()} disabled={creating || remaining > 0} className={PRIMARY_BUTTON}>
              {creating ? 'Creating…' : 'Create group'}
              <Check size={16} aria-hidden="true" />
            </button>
          )}
        </div>
      </div>
    </div>
  )
}
