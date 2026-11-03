/** Shapes mirrored from the Echo backend (backend/retrieval.py, backend/wellbeing.py). */

export type SourceType = 'gmail' | 'chrome' | 'youtube'

/** One item an answer cites (backend rse/llm_synthesizer.py `_source_entry`). */
export interface AnswerSource {
  n: number
  memory_id: string | null
  source_type: SourceType | null
  anchor: string
  date_label: string
  created_at_local: string | null
  title: string
  link: string | null
  role: 'direct' | 'related'
}

export interface QueryResponse {
  final_answer: string
  session_id: string
  no_results: boolean
  result_count: number
  parsed_intent?: Record<string, unknown> | null
  /** The answer with ⟦n⟧ markers that point into `answer_sources` (for the pill UI). */
  answer_markdown?: string
  answer_sources?: AnswerSource[]
}

/** One finished search step from POST /retrieval/query/stream (a fixed label, never question text). */
export interface QueryStep {
  node: string
  label: string
  count: number | null
}

/** One past chat from GET /retrieval/sessions. */
export interface SessionSummary {
  session_id: string
  title: string
  last_active: string
  turns: number
}

/** One stored message of a past chat from GET /retrieval/sessions/{id}. */
export interface SessionMessage {
  role: 'user' | 'echo'
  text: string
  /** D6.1: a reply saved with its pill data carries it back; replies saved earlier have neither. */
  markdown?: string | null
  sources?: AnswerSource[] | null
}

export interface SessionMessages {
  session_id: string
  messages: SessionMessage[]
}

export interface MemoryItem {
  memory_id: string
  source_type: SourceType
  title: string | null
  created_at: string | null
  last_accessed_at?: string | null
  /** Opens the original: the page, the Gmail thread or the YouTube video (D6.7). */
  url?: string | null
  domain?: string | null
  sender?: string | null
  channel_name?: string | null
  is_short?: boolean | null
  /** True while the item is marked as a regret (the Timeline offers "undo" for it). */
  regretted?: boolean
}

/** An item behind one category's time on a day; `total_seconds` is the time it adds to that category. */
export interface CategoryItem extends MemoryItem {
  total_seconds: number
}

/** One page of the newest-first item list; `next_cursor` is null on the last page. */
export interface RecentPage {
  items: MemoryItem[]
  next_cursor: string | null
}

export interface GroupTime {
  group_name: string
  total_seconds: number
  item_count: number
}

export interface DailySummary {
  day: string
  total_seconds: number
  by_system_group: GroupTime[]
  by_source: { source_type: SourceType; total_seconds: number; item_count: number }[]
  shorts: { shorts_count: number; shorts_seconds: number }
  regret: { regret_rate_percent: number; regretted_seconds: number; total_seconds: number }
  sessions: { session_count: number; avg_session_minutes: number; fragmentation: number | null }
}

export interface HeatmapCell {
  day_of_week: number
  hour: number
  item_count: number
  total_seconds: number
  regret_count: number
  has_regret: boolean
}

export interface UserGroup {
  group_id: string
  group_name: string
  description: string | null
  member_count: number
  /** Members the user confirmed; auto-sorting starts at 5 of these (A42.3). Absent from older backends. */
  confirmed_count?: number
  auto_assignment_active: boolean
}

export type RuleType = 'keyword' | 'domain' | 'channel' | 'sender' | 'time_window'

/** One rule of a goal group (backend wba/group_manager.py `list_rules`). */
export interface GroupRule {
  rule_id: string
  group_id: string
  rule_type: RuleType
  rule_value: string
  is_active: boolean
  created_at: string
  match_count: number
}

/** A candidate rule shared by 5+ confirmed members (GET /wellbeing/groups/{id}/rule-suggestions, step 9). */
export interface RuleSuggestion {
  rule_type: 'domain' | 'sender' | 'channel'
  rule_value: string
  match_count: number
}

/** A confirmed member of a goal group (GET /wellbeing/groups/{id}/items). */
export interface GroupMember {
  memory_id: string
  source_type: SourceType
  title: string | null
  created_at: string | null
  assigned_at: string | null
}

/** A member of a goal group with the time spent on it, all time (GET /wellbeing/groups/{id}/time). */
export interface GroupTimedItem extends MemoryItem {
  total_seconds: number
}

/** What the new-goal flow sends: the group with its first items and optional rules, created together. */
export interface NewGroupInput {
  group_name: string
  description: string | null
  memory_ids: string[]
  rules: { rule_type: RuleType; rule_value: string }[]
}

/** The answer of POST /wellbeing/groups/with-items. */
export interface CreatedGroup extends UserGroup {
  rules: GroupRule[]
}

/** The answer of POST /wellbeing/groups/{id}/assign (manual seeding). */
export interface AssignResult {
  memory_id: string
  group_id: string
  member_count: number
  confirmed_count: number
  auto_assignment_active: boolean
}

export interface GroupSuggestion {
  suggestion_id: string
  memory_id: string
  group_id: string
  group_name: string
  title: string | null
  source_type: SourceType
  created_at: string | null
  rule_score: number | null
  knn_score: number | null
  suggested_at: string | null
  decision: string
}

export interface RegrettedItem {
  memory_id: string
  source_type: SourceType
  title: string | null
  created_at: string | null
  latest_note: string | null
  last_marked_at: string | null
  /** The system group the item belongs to; absent from older backends. */
  group_name?: string
}

export interface RegretAnalytics {
  rate: { regret_rate_percent: number; regretted_seconds: number; total_seconds: number }
  by_hour: { hour: number; regret_count: number }[]
  by_category: { group_name: string; regret_count: number; total_seconds: number }[]
  items: RegrettedItem[]
}

/** A stored session (GET /wellbeing/sessions). Times are naive UTC strings. */
export interface SessionRow {
  session_id: string
  session_start: string
  session_end: string
  total_duration_seconds: number
  /** The system group with the most time in the session; null when none of its items has one. */
  dominant_group?: string | null
  /** Distinct items in the session. */
  item_count?: number
  /** Times the source changed between two items (Chrome to YouTube is one). */
  source_switch_count?: number
}

/** An item inside one session (GET /wellbeing/sessions/{id}/items); `activity_at` is its first moment there, naive UTC. */
export interface SessionItem extends MemoryItem {
  total_seconds: number
  activity_at: string
}

/** A regret reminder (GET /wellbeing/regret/reminder): always the user's own earlier note. */
export interface RegretReminder {
  source_type: SourceType
  your_note: string
  options: string[]
}

/** One content type in `ReminderStatus`: could a reminder fire for it, and if not, what is missing. */
export interface ReminderSource {
  source_type: SourceType
  /** Standing flags of this type in the lookback window. */
  flags: number
  /** Local hours (0-23) at which a habit exists, so a reminder could appear. */
  hours: number[]
  /** A currently flagged item of this type has a note the user wrote. */
  has_note: boolean
  ready: boolean
}

/** GET /wellbeing/regret/reminders/status: the switch, today's count and what each type still needs. */
export interface ReminderStatus {
  /** False when the reminder store (Redis) cannot be reached; reminders are off then. */
  available: boolean
  enabled: boolean
  sent_today: number
  daily_limit: number
  min_session_minutes: number
  cooldown_hours: number
  min_flags: number
  lookback_days: number
  sources: ReminderSource[]
}

export interface WeeklyInsight {
  reflection: string
  generated_by: 'llm' | 'deterministic'
  aggregates: {
    week_start: string
    week_end: string
    total_hours: number
    category_breakdown_percent: Record<string, number>
    shorts_count: number
    shorts_time_minutes: number
  }
}

export interface ChatMessage {
  id: string
  role: 'user' | 'echo'
  text: string
  pending?: boolean
  resultCount?: number
  noResults?: boolean
  /** The reply with ⟦n⟧ markers and the items they cite; drawn as memory pills (A27). */
  markdown?: string
  sources?: AnswerSource[]
}
