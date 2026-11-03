import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { motion } from 'framer-motion'
import { ArrowLeft, Plus } from 'lucide-react'
import AddRuleForm from '../components/AddRuleForm'
import PageTitle from '../components/PageTitle'
import Switch from '../components/Switch'
import { addRule, fetchGroupRules, fetchGroups, fetchRuleSuggestions, setRuleActive } from '../lib/api'
import { RULE_KINDS } from '../lib/rules'
import type { GroupRule, RuleSuggestion, RuleType, UserGroup } from '../lib/types'

// A rule that has matched nothing this long after it was made is shown as quiet (D5).
const QUIET_RULE_DAYS = 30

/** Step 9: patterns 5+ confirmed members share, offered as rules. Nothing is added until he clicks. */
function RuleSuggestions({
  suggestions,
  onAccept,
}: {
  suggestions: RuleSuggestion[]
  onAccept: (suggestion: RuleSuggestion) => Promise<void>
}) {
  const [savingKey, setSavingKey] = useState<string | null>(null)
  const [failedKey, setFailedKey] = useState<string | null>(null)

  async function accept(suggestion: RuleSuggestion) {
    const key = `${suggestion.rule_type}:${suggestion.rule_value}`
    if (savingKey) return
    setSavingKey(key)
    setFailedKey(null)
    try {
      await onAccept(suggestion)
    } catch {
      setFailedKey(key)
    } finally {
      setSavingKey(null)
    }
  }

  return (
    <ul>
      {suggestions.map((suggestion) => {
        const key = `${suggestion.rule_type}:${suggestion.rule_value}`
        return (
          <li key={key} className="flex items-center justify-between gap-6 border-b border-white/[0.05] py-3 last:border-0">
            <div className="min-w-0">
              <p className="meta text-[10px]">{RULE_KINDS[suggestion.rule_type].short}</p>
              <p className="mt-1 break-words font-mono text-sm text-ink">{suggestion.rule_value}</p>
              <p className="mt-1 text-xs text-ink-mute">shared by {suggestion.match_count} items in this group</p>
              {failedKey === key && (
                <p role="alert" className="mt-1 text-xs text-youtube">
                  Couldn’t add it. Try again in a moment.
                </p>
              )}
            </div>
            <button
              type="button"
              onClick={() => void accept(suggestion)}
              disabled={savingKey !== null}
              className="btn-ghost inline-flex shrink-0 items-center gap-1.5 text-xs disabled:opacity-40"
              aria-label={`Add rule ${RULE_KINDS[suggestion.rule_type].short} ${suggestion.rule_value}`}
            >
              <Plus size={13} aria-hidden="true" /> {savingKey === key ? 'adding…' : 'add rule'}
            </button>
          </li>
        )
      })}
    </ul>
  )
}

/** The backend sends naive UTC timestamps; read them as UTC rather than local time. */
function ageInDays(createdAt: string): number {
  const hasZone = /[zZ]|[+-]\d\d:?\d\d$/.test(createdAt)
  const created = new Date(hasZone ? createdAt : `${createdAt}Z`).getTime()
  return Number.isNaN(created) ? 0 : (Date.now() - created) / (24 * 3600 * 1000)
}

function isQuiet(rule: GroupRule): boolean {
  return rule.is_active && rule.match_count === 0 && ageInDays(rule.created_at) >= QUIET_RULE_DAYS
}

function RuleRow({
  rule,
  onToggle,
  failed,
}: {
  rule: GroupRule
  onToggle: (rule: GroupRule, active: boolean) => void
  failed: boolean
}) {
  const quiet = isQuiet(rule)
  return (
    <li className="flex items-center justify-between gap-6 border-b border-white/[0.05] py-4 last:border-0">
      <div className={`min-w-0 transition-opacity duration-200 ${rule.is_active ? '' : 'opacity-60'}`}>
        <p className="meta text-[10px]">{RULE_KINDS[rule.rule_type]?.short ?? rule.rule_type}</p>
        <p className="mt-1 break-words font-mono text-sm text-ink">{rule.rule_value}</p>
        <p className="mt-1.5 text-xs leading-5 text-ink-mute">
          {rule.is_active ? 'matching' : 'paused'} · {rule.match_count} {rule.match_count === 1 ? 'match' : 'matches'} so far
          {quiet && <span className="text-ember-300/90"> · no matches in {QUIET_RULE_DAYS}+ days</span>}
        </p>
        {failed && (
          <p role="alert" className="mt-1.5 text-xs text-youtube">
            Couldn’t save that change — it’s back as it was.
          </p>
        )}
      </div>
      <Switch
        checked={rule.is_active}
        onChange={(next) => onToggle(rule, next)}
        label={`${rule.is_active ? 'Pause' : 'Resume'} rule ${rule.rule_value}`}
      />
    </li>
  )
}

/**
 * One goal group's rules, with a pause/resume switch each (D5). Pausing keeps the rule and
 * its match history; a paused rule just stops suggesting items until it is resumed.
 */
export default function GroupRulesPage() {
  const { groupId = '' } = useParams()
  const [group, setGroup] = useState<UserGroup | null>(null)
  const [rules, setRules] = useState<GroupRule[] | null>(null)
  const [failedRuleId, setFailedRuleId] = useState<string | null>(null)
  const [suggestions, setSuggestions] = useState<RuleSuggestion[]>([])

  useEffect(() => {
    let cancelled = false
    setRules(null)
    setSuggestions([])
    void fetchGroups().then((groups) => !cancelled && setGroup(groups.find((g) => g.group_id === groupId) ?? null))
    void fetchGroupRules(groupId).then((data) => !cancelled && setRules(data))
    void fetchRuleSuggestions(groupId).then((data) => !cancelled && setSuggestions(data))
    return () => {
      cancelled = true
    }
  }, [groupId])

  /** Save a rule, then show it; a failure is thrown to the form, which keeps what was typed. */
  async function handleAdd(ruleType: RuleType, ruleValue: string) {
    const created = await addRule(groupId, ruleType, ruleValue)
    setRules((current) => [...(current ?? []), created])
    setSuggestions((current) =>
      current.filter((s) => !(s.rule_type === created.rule_type && s.rule_value === created.rule_value)),
    )
  }

  function setActiveLocally(ruleId: string, active: boolean) {
    setRules((current) => current?.map((r) => (r.rule_id === ruleId ? { ...r, is_active: active } : r)) ?? current)
  }

  function handleToggle(rule: GroupRule, active: boolean) {
    setFailedRuleId(null)
    setActiveLocally(rule.rule_id, active) // optimistic; put back below if the save fails
    void setRuleActive(groupId, rule.rule_id, active).catch(() => {
      setActiveLocally(rule.rule_id, !active)
      setFailedRuleId(rule.rule_id)
    })
  }

  const activeCount = rules?.filter((r) => r.is_active).length ?? 0

  return (
    <div>
      <Link
        to="/groups"
        className="meta mb-6 inline-flex items-center gap-1.5 text-ink-mute transition hover:text-ink-soft"
      >
        <ArrowLeft size={12} aria-hidden="true" /> groups
      </Link>

      <PageTitle
        eyebrow={group ? `group rules · ${group.group_name}` : 'group rules'}
        title="How it sorts."
        aside={
          <div className="flex items-center gap-4">
            {rules && <p className="meta">{activeCount} of {rules.length} matching</p>}
            <Link
              to={`/groups/${groupId}/items`}
              className="meta text-ember-400 transition hover:text-ember-300"
            >
              items →
            </Link>
          </div>
        }
      />

      <motion.p
        className="mt-6 max-w-[56ch] text-[15px] leading-7 text-ink-mute"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.6, delay: 0.35 }}
      >
        A rule tells Echo what to look for when it suggests items for this group. Pausing one keeps it and
        everything it matched; Echo just stops using it until you switch it back on.
      </motion.p>

      <section className="card mt-10 max-w-3xl px-6 py-2" aria-label="Rules in this group">
        {rules === null ? (
          <div className="space-y-4 py-4" role="status" aria-label="Loading rules">
            {[0, 1, 2].map((index) => (
              <div key={index} className="h-12 animate-pulse rounded bg-night-800/60" />
            ))}
          </div>
        ) : rules.length === 0 ? (
          <p className="py-6 text-sm text-ink-mute">No rules for this group yet.</p>
        ) : (
          <ul>
            {rules.map((rule) => (
              <RuleRow key={rule.rule_id} rule={rule} onToggle={handleToggle} failed={failedRuleId === rule.rule_id} />
            ))}
          </ul>
        )}
      </section>

      <section className="card mt-6 max-w-3xl px-6 py-5" aria-labelledby="add-rule-heading">
        <h2 id="add-rule-heading" className="meta">
          add a rule
        </h2>
        <AddRuleForm onAdd={handleAdd} />
      </section>

      {suggestions.length > 0 && (
        <section className="card mt-6 max-w-3xl px-6 py-4" aria-labelledby="rule-suggestions-heading">
          <h2 id="rule-suggestions-heading" className="meta">
            rules echo noticed · {suggestions.length}
          </h2>
          <p className="mt-2 text-xs leading-5 text-ink-mute">
            Five or more items you put in this group share these. Nothing changes unless you add one.
          </p>
          <RuleSuggestions
            suggestions={suggestions}
            onAccept={(suggestion) => handleAdd(suggestion.rule_type, suggestion.rule_value)}
          />
        </section>
      )}
    </div>
  )
}
