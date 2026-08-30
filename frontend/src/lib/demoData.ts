/**
 * Demo dataset — used automatically when the Echo backend is unreachable so
 * the interface is always explorable. Shapes mirror lib/types.ts exactly.
 */
import type {
  DailySummary,
  HeatmapCell,
  MemoryItem,
  QueryResponse,
  RegretAnalytics,
  UserGroup,
  WeeklyInsight,
} from './types'

const now = Date.now()
const hoursAgo = (h: number) => new Date(now - h * 3.6e6).toISOString()

export const demoRecent: MemoryItem[] = [
  { memory_id: 'd1', source_type: 'gmail', title: 'TechCorp interview — next steps & availability', created_at: hoursAgo(3), last_accessed_at: hoursAgo(2), sender: 'recruiting@techcorp.com' },
  { memory_id: 'd2', source_type: 'chrome', title: 'Memory management: paging vs segmentation', created_at: hoursAgo(5), last_accessed_at: hoursAgo(5), domain: 'geeksforgeeks.org', url: 'https://geeksforgeeks.org' },
  { memory_id: 'd3', source_type: 'youtube', title: 'OS Scheduling Algorithms — visual walkthrough', created_at: hoursAgo(7), last_accessed_at: hoursAgo(6), channel_name: 'CS Primer' },
  { memory_id: 'd4', source_type: 'chrome', title: 'CFS scheduler internals — kernel newbies', created_at: hoursAgo(26), last_accessed_at: hoursAgo(25), domain: 'kernelnewbies.org' },
  { memory_id: 'd5', source_type: 'gmail', title: 'PES University — capstone review 2 schedule', created_at: hoursAgo(30), last_accessed_at: hoursAgo(8), sender: 'capstone@pes.edu' },
  { memory_id: 'd6', source_type: 'youtube', title: 'Virtual memory, explained like a story', created_at: hoursAgo(49), last_accessed_at: hoursAgo(48), channel_name: '3Blue1Brown' },
  { memory_id: 'd7', source_type: 'chrome', title: 'System design primer — caching strategies', created_at: hoursAgo(52), last_accessed_at: hoursAgo(51), domain: 'github.com' },
  { memory_id: 'd8', source_type: 'youtube', title: 'lofi — midnight study session', created_at: hoursAgo(54), last_accessed_at: hoursAgo(53), channel_name: 'Chillhop', is_short: false },
]

export const demoDaily: DailySummary = {
  day: new Date().toISOString().slice(0, 10),
  total_seconds: 4 * 3600 + 23 * 60,
  by_system_group: [
    { group_name: 'work', total_seconds: 3120, item_count: 6 },
    { group_name: 'study', total_seconds: 7980, item_count: 14 },
    { group_name: 'entertainment', total_seconds: 3300, item_count: 9 },
    { group_name: 'personal', total_seconds: 900, item_count: 2 },
    { group_name: 'misc', total_seconds: 480, item_count: 3 },
  ],
  by_source: [
    { source_type: 'gmail', total_seconds: 1860, item_count: 8 },
    { source_type: 'chrome', total_seconds: 8760, item_count: 17 },
    { source_type: 'youtube', total_seconds: 5160, item_count: 9 },
  ],
  shorts: { shorts_count: 11, shorts_seconds: 940 },
  regret: { regret_rate_percent: 12.4, regretted_seconds: 1960, total_seconds: 15780 },
  sessions: { session_count: 9, avg_session_minutes: 24.6, fragmentation: 0.37 },
}

export const demoHeatmap: HeatmapCell[] = Array.from({ length: 7 }, (_, d) =>
  Array.from({ length: 24 }, (_, h) => {
    const evening = h >= 19 && h <= 24 ? 2.2 : 1
    const afternoon = h >= 14 && h <= 17 ? 1.7 : 1
    const base = h >= 8 && h <= 23 ? Math.sin(((h - 8) / 15) * Math.PI) : 0
    const seconds = Math.round(base * evening * afternoon * 1400 * (0.6 + ((d * 7 + h) % 5) * 0.18))
    return {
      day_of_week: d,
      hour: h,
      item_count: Math.round(seconds / 400),
      total_seconds: seconds,
      regret_count: h >= 23 && d >= 4 ? 1 : 0,
      has_regret: h >= 23 && d >= 4,
    }
  }),
).flat()

export const demoGroups: UserGroup[] = [
  { group_id: 'g1', group_name: 'Capstone Project', description: 'Everything feeding the final-year build', member_count: 23, auto_assignment_active: true },
  { group_id: 'g2', group_name: 'Placement Prep', description: 'Interview practice, OS revision, company research', member_count: 14, auto_assignment_active: true },
  { group_id: 'g3', group_name: 'Health Research', description: null, member_count: 4, auto_assignment_active: false },
]

export const demoRegret: RegretAnalytics = {
  rate: { regret_rate_percent: 12.4, regretted_seconds: 1960, total_seconds: 15780 },
  by_hour: [
    { hour: 23, regret_count: 6 },
    { hour: 0, regret_count: 4 },
    { hour: 22, regret_count: 3 },
    { hour: 15, regret_count: 1 },
  ],
  by_category: [
    { group_name: 'entertainment', regret_count: 9, total_seconds: 8340 },
    { group_name: 'misc', regret_count: 3, total_seconds: 1260 },
    { group_name: 'study', regret_count: 1, total_seconds: 420 },
  ],
  items: [
    { memory_id: 'r1', source_type: 'youtube', title: 'Shorts session — late night', created_at: hoursAgo(28), latest_note: 'an hour before the exam, again', last_marked_at: hoursAgo(27) },
    { memory_id: 'r2', source_type: 'chrome', title: 'Endless feed scroll — r/all', created_at: hoursAgo(50), latest_note: 'meant to read one thread', last_marked_at: hoursAgo(49) },
    { memory_id: 'r3', source_type: 'youtube', title: 'Autoplay rabbit hole after tutorial', created_at: hoursAgo(75), latest_note: null, last_marked_at: hoursAgo(74) },
  ],
}

export const demoInsight: WeeklyInsight = {
  reflection:
    'This week held 22.4 hours of tracked activity, and more than half of it — 54% — went to study. Your regret marks cluster after 11 PM, mostly around Shorts sessions, and each one carried your own note rather than a judgment. The email → article → video chain appeared four times, usually beginning with a capstone or placement thread in the afternoon.',
  generated_by: 'llm',
  aggregates: {
    week_start: new Date(now - 6 * 864e5).toISOString().slice(0, 10),
    week_end: new Date().toISOString().slice(0, 10),
    total_hours: 22.4,
    category_breakdown_percent: { study: 54, entertainment: 21, work: 17, personal: 5, misc: 3 },
    shorts_count: 46,
    shorts_time_minutes: 118,
  },
}

export function demoQueryReply(query: string): QueryResponse {
  return {
    final_answer:
      `Here's what your memory holds for “${query}”. The TechCorp interview email arrived this morning [1] — after it, ` +
      'you read a paging-versus-segmentation article on GeeksforGeeks [2] and watched a scheduling-algorithms walkthrough [3]. ' +
      'The three sit inside one focused study session, about forty minutes end to end. (Demo answer — start the Echo backend for real recall.)',
    session_id: 'demo-session',
    no_results: false,
    result_count: 3,
    parsed_intent: { sources: ['gmail', 'chrome', 'youtube'], query_clean: query },
  }
}
