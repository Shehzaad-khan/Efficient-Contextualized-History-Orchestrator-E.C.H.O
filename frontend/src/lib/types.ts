/** Shapes mirrored from the Echo backend (backend/retrieval.py, backend/wellbeing.py). */

export type SourceType = 'gmail' | 'chrome' | 'youtube'

export interface QueryResponse {
  final_answer: string
  session_id: string
  no_results: boolean
  result_count: number
  parsed_intent?: Record<string, unknown> | null
}

export interface MemoryItem {
  memory_id: string
  source_type: SourceType
  title: string | null
  created_at: string | null
  last_accessed_at?: string | null
  url?: string | null
  domain?: string | null
  sender?: string | null
  channel_name?: string | null
  is_short?: boolean | null
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
}

export interface RegretAnalytics {
  rate: { regret_rate_percent: number; regretted_seconds: number; total_seconds: number }
  by_hour: { hour: number; regret_count: number }[]
  by_category: { group_name: string; regret_count: number; total_seconds: number }[]
  items: RegrettedItem[]
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
}
