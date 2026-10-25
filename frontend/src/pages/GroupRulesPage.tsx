import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { motion } from 'framer-motion'
import { ArrowLeft } from 'lucide-react'
import PageTitle from '../components/PageTitle'
import Switch from '../components/Switch'
import { fetchGroupRules, fetchGroups, setRuleActive } from '../lib/api'
import type { GroupRule, UserGroup } from '../lib/types'

// A rule that has matched nothing this long after it was made is shown as quiet (D5).
const QUIET_RULE_DAYS = 30

const RULE_TYPE_LABEL: Record<GroupRule['rule_type'], string> = {
  keyword: 'title has',
  domain: 'site',
  channel: 'channel',
  sender: 'sender',
  time_window: 'dates',
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
        <p className="meta text-[10px]">{RULE_TYPE_LABEL[rule.rule_type] ?? rule.rule_type}</p>
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

  useEffect(() => {
    let cancelled = false
    setRules(null)
    void fetchGroups().then((groups) => !cancelled && setGroup(groups.find((g) => g.group_id === groupId) ?? null))
    void fetchGroupRules(groupId).then((data) => !cancelled && setRules(data))
    return () => {
      cancelled = true
    }
  }, [groupId])

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
        to="/patterns"
        className="meta mb-6 inline-flex items-center gap-1.5 text-ink-mute transition hover:text-ink-soft"
      >
        <ArrowLeft size={12} aria-hidden="true" /> patterns
      </Link>

      <PageTitle
        eyebrow={group ? `group rules · ${group.group_name}` : 'group rules'}
        title="How it sorts."
        aside={rules ? <p className="meta">{activeCount} of {rules.length} matching</p> : undefined}
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
    </div>
  )
}
