import { useEffect, useMemo, useState, type ReactNode } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import { ArrowLeft, ArrowRight, Check, Plus, Search, Target, X } from 'lucide-react'
import AddRuleForm from '../components/AddRuleForm'
import PageTitle from '../components/PageTitle'
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
    <ol className="flex flex-wrap items-center gap-x-2 gap-y-2" aria-label="Steps">
      {STEPS.map((label, index) => {
        const done = index < current
        const active = index === current
        return (
          <li key={label} className="flex items-center gap-2" aria-current={active ? 'step' : undefined}>
            <span
              className={`flex h-6 w-6 items-center justify-center rounded-full border font-mono text-[11px] transition-colors duration-200 ${
                active
                  ? 'border-ember-500 bg-ember-500 text-night-950'
                  : done
                    ? 'border-ember-500/50 bg-ember-500/10 text-ember-400'
                    : 'border-white/10 text-ink-faint'
              }`}
            >
              {done ? <Check size={12} aria-hidden="true" /> : index + 1}
            </span>
            <span className={`meta text-[11px] ${active ? 'text-ember-300' : done ? 'text-ink-soft' : 'text-ink-faint'}`}>
              {label}
            </span>
            {index < STEPS.length - 1 && <span aria-hidden="true" className="mx-1 h-px w-5 bg-white/10" />}
          </li>
        )
      })}
    </ol>
  )
}

function StepHeading({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div>
      <h2 className="font-display text-2xl italic text-ink" style={{ fontVariationSettings: '"opsz" 36, "SOFT" 40' }}>
        {title}
      </h2>
      <p className="mt-2 max-w-[60ch] text-[15px] leading-7 text-ink-mute">{children}</p>
    </div>
  )
}

const PRIMARY_BUTTON =
  'inline-flex items-center gap-2 rounded-pill bg-ember-500 px-5 py-2.5 text-sm font-semibold text-night-950 transition duration-200 ease-out hover:bg-ember-400 active:scale-[0.97] disabled:cursor-not-allowed disabled:bg-white/[0.06] disabled:text-ink-faint disabled:hover:bg-white/[0.06] disabled:active:scale-100'

/**
 * The guided "new goal" flow. A group does not exist until the last step: the name, the examples
 * and the hints live only in this page, and one call at the end creates all of it or nothing.
 * Leaving earlier creates nothing.
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

  useEffect(() => {
    let cancelled = false
    void fetchRecent(PICKER_ITEMS).then((data) => !cancelled && setRecent(data))
    return () => {
      cancelled = true
    }
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
      navigate('/patterns', {
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
    navigate('/patterns')
  }

  return (
    <div>
      <button
        type="button"
        onClick={leave}
        className="meta mb-6 inline-flex items-center gap-1.5 text-ink-mute transition hover:text-ink-soft"
      >
        <ArrowLeft size={12} aria-hidden="true" /> patterns
      </button>

      <PageTitle eyebrow="new goal group" title="Start a goal." />

      <div className="mt-6 max-w-3xl">
        <StepIndicator current={step} />

        {leaving && (
          <div role="alert" className="mt-5 rounded-card border border-youtube/30 bg-night-900/60 px-4 py-3">
            <p className="text-sm text-ink-soft">Leave without creating it? Nothing has been saved yet, so the draft is lost.</p>
            <div className="mt-2 flex gap-2">
              <button
                type="button"
                onClick={() => navigate('/patterns')}
                className="btn-ghost border-youtube/40 px-3 py-1 font-mono text-xs normal-case text-youtube hover:border-youtube/70"
              >
                yes, leave
              </button>
              <button type="button" onClick={() => setLeaving(false)} className="btn-ghost px-3 py-1 font-mono text-xs normal-case">
                keep going
              </button>
            </div>
          </div>
        )}

        <AnimatePresence mode="wait">
          <motion.section
            key={step}
            className="card mt-6 px-6 py-6"
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -6, transition: { duration: 0.12 } }}
            transition={{ duration: 0.25, ease: EASE_OUT }}
            aria-label={STEPS[step]}
          >
            {step === 0 && (
              <div>
                <StepHeading title="What are you working towards?">
                  A goal group collects the pages, emails and videos that belong to one goal. Echo learns from your
                  examples, then suggests more.
                </StepHeading>

                <label htmlFor="new-group-name" className="meta mt-6 mb-2 block">
                  name
                </label>
                <div className="flex items-center gap-2 rounded-pill border border-ember-500/25 bg-night-900 px-4 py-1.5 transition duration-200 focus-within:border-ember-500/50 focus-within:shadow-[0_0_0_3px_rgba(245,169,71,0.12)]">
                  <Target size={16} className="shrink-0 text-ember-400" aria-hidden="true" />
                  <input
                    id="new-group-name"
                    value={name}
                    onChange={(event) => setName(event.target.value)}
                    maxLength={100}
                    autoFocus
                    placeholder="e.g. Placement Prep"
                    className="min-w-0 flex-1 bg-transparent py-1.5 text-sm text-ink placeholder:font-mono placeholder:text-xs placeholder:text-ink-faint focus:outline-none"
                  />
                </div>
                {trimmedName === '' && (
                  <div className="mt-3 flex flex-wrap items-center gap-2">
                    <span className="meta text-[10px] text-ink-faint">try</span>
                    {NAME_EXAMPLES.map((example) => (
                      <button
                        key={example}
                        type="button"
                        onClick={() => setName(example)}
                        className="rounded-pill border border-white/10 px-3 py-1 font-mono text-[11px] text-ink-mute transition hover:border-ember-500/30 hover:text-ink-soft"
                      >
                        {example}
                      </button>
                    ))}
                  </div>
                )}

                <label htmlFor="new-group-description" className="meta mt-6 mb-2 block">
                  one line about it · optional
                </label>
                <input
                  id="new-group-description"
                  value={description}
                  onChange={(event) => setDescription(event.target.value)}
                  maxLength={500}
                  placeholder="e.g. everything for the campus drive in January"
                  className="input-study px-4 py-2.5 text-sm"
                />

                <div className="mt-7 rounded-card border border-white/[0.06] bg-night-900/50 px-4 py-4">
                  <p className="meta text-[10px]">how it works</p>
                  <ol className="mt-3 space-y-2.5 text-sm leading-6 text-ink-soft">
                    <li>
                      <span className="mr-2 font-mono text-ember-400">1</span>You pick {EXAMPLES_NEEDED} things you have already seen that belong to this goal.
                    </li>
                    <li>
                      <span className="mr-2 font-mono text-ember-400">2</span>Optionally, you tell Echo what else to look for: a website, a sender, a channel.
                    </li>
                    <li>
                      <span className="mr-2 font-mono text-ember-400">3</span>From then on Echo suggests matching items in your Sunday review. Nothing joins
                      without your yes, except very sure matches, and those you can undo in one click.
                    </li>
                  </ol>
                </div>
              </div>
            )}

            {step === 1 && (
              <div>
                <StepHeading title={`Pick ${EXAMPLES_NEEDED} examples.`}>
                  Choose things you have already seen that belong to “{trimmedName}”. {EXAMPLES_NEEDED} is enough to
                  start; more is optional, and more examples help Echo learn.
                </StepHeading>

                <div className="mt-5" aria-live="polite">
                  <div className="flex items-baseline justify-between gap-4">
                    <p className="font-mono text-sm text-ink">
                      {picked.length} <span className="text-ink-mute">picked</span>
                    </p>
                    <p className={`meta text-[11px] ${remaining === 0 ? 'text-moss' : 'text-ember-400'}`}>
                      {remaining === 0
                        ? picked.length === EXAMPLES_NEEDED
                          ? 'enough to start · add more if you like'
                          : 'enough to start'
                        : `${remaining} more to go`}
                    </p>
                  </div>
                  <div className="mt-2 flex gap-1.5" aria-hidden="true">
                    {Array.from({ length: EXAMPLES_NEEDED }, (_, index) => (
                      <span
                        key={index}
                        className={`h-1.5 flex-1 rounded-full transition-colors duration-300 ${
                          index < picked.length ? 'bg-gradient-to-r from-ember-600 to-ember-400' : 'bg-night-700/60'
                        }`}
                      />
                    ))}
                  </div>
                </div>

                <label className="input-study mt-5 flex items-center gap-2 px-3 py-2">
                  <Search size={13} aria-hidden="true" className="shrink-0 text-ink-faint" />
                  <input
                    value={search}
                    onChange={(event) => setSearch(event.target.value)}
                    placeholder="filter by title, sender, site or channel…"
                    aria-label="Filter recent items"
                    className="flex-1 bg-transparent text-sm text-ink placeholder:text-ink-faint focus:outline-none"
                  />
                </label>

                {recent === null ? (
                  <div className="mt-4 space-y-3" role="status" aria-label="Loading recent items">
                    {[0, 1, 2, 3].map((index) => (
                      <div key={index} className="h-12 animate-pulse rounded bg-night-800/60" />
                    ))}
                  </div>
                ) : candidates.length === 0 ? (
                  <p className="mt-4 text-sm text-ink-mute">
                    {recent.length === 0
                      ? 'Nothing captured yet. Browse for a while, then come back.'
                      : 'Nothing in your recent items matches that.'}
                  </p>
                ) : (
                  <ScrollList label="Recent items you can pick">
                    {candidates.map((item) => {
                      const isPicked = pickedSet.has(item.memory_id)
                      return (
                        <li key={item.memory_id} className="border-b border-white/[0.05] last:border-0">
                          <button
                            type="button"
                            role="checkbox"
                            aria-checked={isPicked}
                            onClick={() => togglePick(item.memory_id)}
                            className="flex w-full items-center gap-4 rounded-sm py-3 text-left transition hover:bg-white/[0.02] focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ember-500/40"
                          >
                            <span
                              aria-hidden="true"
                              className={`flex h-5 w-5 shrink-0 items-center justify-center rounded-[6px] border transition-colors duration-150 ${
                                isPicked ? 'border-ember-500 bg-ember-500 text-night-950' : 'border-white/20 text-transparent'
                              }`}
                            >
                              <Check size={13} />
                            </span>
                            <span className="min-w-0 flex-1">
                              <span className="flex flex-wrap items-center gap-2">
                                <SourceBadge source={item.source_type} />
                                <span className="meta text-ink-faint">
                                  {formatWhen(asUtc(item.last_accessed_at ?? item.created_at))}
                                </span>
                              </span>
                              <span className={`mt-1 block truncate text-sm ${isPicked ? 'text-ink' : 'text-ink-soft'}`}>
                                {item.title || '(untitled)'}
                              </span>
                              {itemLocation(item) && (
                                <span className="block truncate text-xs text-ink-mute">{itemLocation(item)}</span>
                              )}
                            </span>
                          </button>
                        </li>
                      )
                    })}
                  </ScrollList>
                )}
                <p className="meta mt-3 text-[9px] text-ink-faint">
                  your {PICKER_ITEMS} most recent items · search Recall for older ones
                </p>
              </div>
            )}

            {step === 2 && (
              <div>
                <StepHeading title="Give Echo some hints.">
                  Hints tell Echo what else to look for. They are optional: skip this if you are not sure, and add or
                  change them any time from the group’s page.
                </StepHeading>

                {suggestions.length > 0 && (
                  <div className="mt-6">
                    <p className="meta text-[10px]">from your {picked.length} examples · tap to add</p>
                    <ul className="mt-3 flex flex-wrap gap-2">
                      {suggestions.map((suggestion) => {
                        const on = hintKeys.has(ruleKey(suggestion.type, suggestion.value))
                        return (
                          <li key={ruleKey(suggestion.type, suggestion.value)}>
                            <button
                              type="button"
                              aria-pressed={on}
                              onClick={() => (on ? removeHint(suggestion) : addHint(suggestion.type, suggestion.value))}
                              className={`inline-flex items-center gap-2 rounded-pill border px-3.5 py-1.5 text-left text-sm transition duration-200 ${
                                on
                                  ? 'border-ember-500/60 bg-ember-500/15 text-ember-300'
                                  : 'border-white/10 text-ink-soft hover:border-ember-500/30'
                              }`}
                            >
                              {on ? <Check size={13} aria-hidden="true" /> : <Plus size={13} aria-hidden="true" />}
                              <span>{ruleSentence(suggestion.type, suggestion.value)}</span>
                              <span className="font-mono text-[11px] text-ink-faint">{suggestion.count} of your picks</span>
                            </button>
                          </li>
                        )
                      })}
                    </ul>
                  </div>
                )}

                <div className="mt-6">
                  <p className="meta text-[10px]">or add your own</p>
                  <AddRuleForm onAdd={addHintFromForm} />
                </div>

                <div className="mt-6">
                  <p className="meta text-[10px]">
                    {hints.length === 0 ? 'no hints yet · that is fine' : `${hints.length} ${hints.length === 1 ? 'hint' : 'hints'} so far`}
                  </p>
                  {hints.length > 0 && (
                    <ul className="mt-2">
                      {hints.map((hint) => (
                        <li
                          key={ruleKey(hint.type, hint.value)}
                          className="flex items-center justify-between gap-4 border-b border-white/[0.05] py-2.5 last:border-0"
                        >
                          <span className="min-w-0 text-sm text-ink-soft">
                            <span className="meta mr-2 text-[10px]">{RULE_KINDS[hint.type].short}</span>
                            <span className="break-words font-mono text-ink">{hint.type === 'time_window' ? hint.value.replace(',', ' → ') : hint.value}</span>
                          </span>
                          <button
                            type="button"
                            onClick={() => removeHint(hint)}
                            aria-label={`Remove hint: ${ruleSentence(hint.type, hint.value)}`}
                            className="shrink-0 rounded-full p-1.5 text-ink-faint transition hover:text-youtube"
                          >
                            <X size={14} aria-hidden="true" />
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

                <dl className="mt-6 space-y-5">
                  <div>
                    <dt className="meta text-[10px]">goal</dt>
                    <dd className="mt-1 text-base font-medium text-ink">{trimmedName}</dd>
                    {description.trim() && <dd className="mt-0.5 text-sm text-ink-mute">{description.trim()}</dd>}
                  </div>
                  <div>
                    <dt className="meta text-[10px]">
                      {picked.length} {picked.length === 1 ? 'example' : 'examples'}
                    </dt>
                    <dd>
                      <ul className="mt-1.5 space-y-1.5">
                        {pickedItems.slice(0, 6).map((item) => (
                          <li key={item.memory_id} className="flex items-center gap-3">
                            <SourceBadge source={item.source_type} />
                            <span className="min-w-0 flex-1 truncate text-sm text-ink-soft">{item.title || '(untitled)'}</span>
                          </li>
                        ))}
                      </ul>
                      {pickedItems.length > 6 && (
                        <p className="mt-1.5 font-mono text-xs text-ink-mute">and {pickedItems.length - 6} more</p>
                      )}
                    </dd>
                  </div>
                  <div>
                    <dt className="meta text-[10px]">hints</dt>
                    <dd className="mt-1.5 text-sm text-ink-soft">
                      {hints.length === 0 ? (
                        <span className="text-ink-mute">none. Echo will learn from your examples alone.</span>
                      ) : (
                        <ul className="space-y-1">
                          {hints.map((hint) => (
                            <li key={ruleKey(hint.type, hint.value)}>Looks for {ruleSentence(hint.type, hint.value)}</li>
                          ))}
                        </ul>
                      )}
                    </dd>
                  </div>
                </dl>

                <div className="mt-7 rounded-card border border-white/[0.06] bg-night-900/50 px-4 py-4 text-sm leading-6 text-ink-soft">
                  <p className="meta text-[10px]">what happens next</p>
                  <p className="mt-2">
                    Echo starts suggesting similar items right away. You will see them in the Sunday review on Patterns,
                    and nothing is added without your yes, except very sure matches, which you can undo.
                  </p>
                </div>

                {error && (
                  <p role="alert" className="mt-5 text-sm text-youtube">
                    {error}
                  </p>
                )}
              </div>
            )}

            <div className="mt-8 flex flex-wrap items-center justify-between gap-3 border-t border-white/[0.05] pt-5">
              {step > 0 ? (
                <button
                  type="button"
                  onClick={() => {
                    setStep(step - 1)
                    setError(null)
                  }}
                  disabled={creating}
                  className="btn-ghost inline-flex items-center gap-1.5 disabled:opacity-40"
                >
                  <ArrowLeft size={14} aria-hidden="true" /> back
                </button>
              ) : (
                <Link to="/patterns" className="meta text-ink-mute transition hover:text-ink-soft" onClick={(event) => {
                  if (hasDraft) {
                    event.preventDefault()
                    setLeaving(true)
                  }
                }}>
                  cancel
                </Link>
              )}

              {step < STEPS.length - 1 ? (
                <div className="flex items-center gap-4">
                  {step === 2 && hints.length === 0 && (
                    <span className="meta text-[10px] text-ink-faint">skipping is fine</span>
                  )}
                  <button type="button" onClick={() => setStep(step + 1)} disabled={!canContinue} className={PRIMARY_BUTTON}>
                    {step === 0 ? 'next: pick examples' : step === 1 ? (remaining > 0 ? `pick ${remaining} more` : 'next: add hints') : hints.length === 0 ? 'skip hints' : 'next: review'}
                    <ArrowRight size={14} aria-hidden="true" />
                  </button>
                </div>
              ) : (
                <button type="button" onClick={() => void create()} disabled={creating || remaining > 0} className={PRIMARY_BUTTON}>
                  {creating ? 'creating…' : 'create group'}
                  <Check size={14} aria-hidden="true" />
                </button>
              )}
            </div>
          </motion.section>
        </AnimatePresence>
      </div>
    </div>
  )
}
