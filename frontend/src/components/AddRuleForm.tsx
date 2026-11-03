import { useState } from 'react'
import { Plus } from 'lucide-react'
import { ApiError } from '../lib/api'
import { RULE_KINDS, RULE_TYPES } from '../lib/rules'
import type { RuleType } from '../lib/types'

/** A refused rule's reason from the backend, else a plain line. */
function ruleErrorText(error: unknown): string {
  if (error instanceof ApiError && error.status === 400 && error.detail) return error.detail
  if (error instanceof ApiError && error.status === 404) return 'This group no longer exists.'
  return 'Couldn’t save the hint. Try again in a moment.'
}

/**
 * The form that adds a rule: pick what to look for, type it, add. `onAdd` does the saving
 * (to the backend on the rules page, to the draft in the new-group flow) and throws to refuse;
 * the form keeps what was typed and shows the reason.
 */
export default function AddRuleForm({ onAdd }: { onAdd: (type: RuleType, value: string) => Promise<void> }) {
  const [ruleType, setRuleType] = useState<RuleType>('domain')
  const [value, setValue] = useState('')
  const [start, setStart] = useState('')
  const [end, setEnd] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const isWindow = ruleType === 'time_window'
  const ruleValue = isWindow ? (start && end ? `${start},${end}` : '') : value.trim()

  async function submit() {
    if (!ruleValue || saving) return
    if (isWindow && start > end) {
      setError('The first day must not be after the last day.')
      return
    }
    setSaving(true)
    setError(null)
    try {
      await onAdd(ruleType, ruleValue)
      setValue('')
      setStart('')
      setEnd('')
    } catch (failure) {
      setError(ruleErrorText(failure))
    } finally {
      setSaving(false)
    }
  }

  return (
    <form
      className="mt-2"
      onSubmit={(event) => {
        event.preventDefault()
        void submit()
      }}
      aria-label="Add a hint"
    >
      <div className="flex flex-wrap items-center gap-2">
        <select
          value={ruleType}
          onChange={(event) => {
            setRuleType(event.target.value as RuleType)
            setError(null)
          }}
          aria-label="What should Echo look for?"
          className="input-study w-auto px-3 py-2 text-sm"
        >
          {RULE_TYPES.map((type) => (
            <option key={type} value={type}>
              {RULE_KINDS[type].label}
            </option>
          ))}
        </select>
        {isWindow ? (
          <>
            <input
              type="date"
              value={start}
              onChange={(event) => setStart(event.target.value)}
              aria-label="First day"
              className="input-study w-auto px-3 py-2 text-sm"
            />
            <span className="meta text-ink-faint">to</span>
            <input
              type="date"
              value={end}
              onChange={(event) => setEnd(event.target.value)}
              aria-label="Last day"
              className="input-study w-auto px-3 py-2 text-sm"
            />
          </>
        ) : (
          <input
            value={value}
            onChange={(event) => setValue(event.target.value)}
            placeholder={RULE_KINDS[ruleType].placeholder}
            aria-label={RULE_KINDS[ruleType].label}
            maxLength={500}
            className="input-study w-auto min-w-40 flex-1 px-3 py-2 text-sm"
          />
        )}
        <button
          type="submit"
          disabled={saving || !ruleValue}
          className="btn-ghost inline-flex items-center gap-1.5 disabled:opacity-40"
          aria-label="Add hint"
        >
          <Plus size={14} aria-hidden="true" /> {saving ? 'adding…' : 'add'}
        </button>
      </div>
      <p className="mt-2 text-xs leading-5 text-ink-mute">{RULE_KINDS[ruleType].hint}</p>
      {error && (
        <p role="alert" className="mt-1.5 text-xs text-youtube">
          {error}
        </p>
      )}
    </form>
  )
}
