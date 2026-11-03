/**
 * Echo backend client.
 *
 * Every call races a timeout; on network failure the app flips into demo mode
 * (visible in the shell) and callers receive seeded data so the interface is
 * always explorable. Demo mode is sticky per session but re-probes on demand.
 */
import {
  demoDaily,
  demoGroups,
  demoHeatmap,
  demoInsight,
  demoQueryReply,
  demoRecent,
  demoRegret,
  demoRules,
} from './demoData'
import type {
  AssignResult,
  CategoryItem,
  DailySummary,
  GroupMember,
  GroupRule,
  GroupSuggestion,
  HeatmapCell,
  MemoryItem,
  QueryResponse,
  QueryStep,
  RecentPage,
  RegretAnalytics,
  RegretReminder,
  RuleSuggestion,
  RuleType,
  SessionRow,
  SourceType,
  SessionMessages,
  SessionSummary,
  UserGroup,
  WeeklyInsight,
} from './types'

const API_BASE: string = (import.meta.env.VITE_API_BASE as string | undefined) ?? 'http://localhost:8000'

type Listener = (demo: boolean) => void

let demoMode = false
const listeners = new Set<Listener>()

export function isDemoMode(): boolean {
  return demoMode
}

export function onDemoModeChange(listener: Listener): () => void {
  listeners.add(listener)
  return () => listeners.delete(listener)
}

function setDemoMode(value: boolean): void {
  if (demoMode === value) return
  demoMode = value
  listeners.forEach((listener) => listener(value))
}

/**
 * A refused call. `detail` is the backend's own message (FastAPI's `detail`) when
 * it sent a readable one, e.g. a malformed time window on a new rule (400).
 */
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly detail: string | null,
    message: string,
  ) {
    super(message)
  }
}

async function errorFrom(response: Response): Promise<ApiError> {
  let detail: string | null = null
  try {
    const body = (await response.json()) as { detail?: unknown }
    if (typeof body.detail === 'string') detail = body.detail
  } catch {
    // Not JSON (a proxy page, an empty body): keep the status line only.
  }
  return new ApiError(response.status, detail, `${response.status} ${response.statusText}`)
}

// Data calls hit a remote Postgres (Neon, us-east-1) and can legitimately take
// several seconds; the timeout only guards against a truly hung socket, so it
// sits well above realistic latency rather than racing it.
async function request<T>(path: string, init?: RequestInit, timeoutMs = 20000): Promise<T> {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const response = await fetch(`${API_BASE}${path}`, {
      ...init,
      signal: controller.signal,
      headers: { 'Content-Type': 'application/json', ...(init?.headers ?? {}) },
    })
    if (!response.ok) throw await errorFrom(response)
    setDemoMode(false) // a live response is proof the backend is up — heal the badge
    return (await response.json()) as T
  } finally {
    clearTimeout(timer)
  }
}

/**
 * Liveness is decided by /health — a DB-free endpoint that answers in
 * milliseconds — NOT by whether a slow data query happened to beat its timeout.
 * That decoupling is the fix for the old "one slow call flips the whole app to
 * demo forever" behaviour: a heavy query stalling never lies about the backend
 * being offline, and recovery is automatic once /health answers again.
 */
export async function probeBackend(): Promise<boolean> {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), 4000)
  try {
    const response = await fetch(`${API_BASE}/health`, { signal: controller.signal })
    const live = response.ok
    setDemoMode(!live)
    return live
  } catch {
    setDemoMode(true)
    return false
  } finally {
    clearTimeout(timer)
  }
}

/**
 * Wrap a live call with its demo fallback. When the backend is known-down we
 * short-circuit to demo data so widgets don't each wait out the full timeout.
 * Otherwise we always attempt the live call; a failure degrades THIS widget to
 * demo data and re-probes /health to settle the real liveness state, but never
 * unilaterally declares the backend offline off a single slow query.
 */
async function withFallback<T>(live: () => Promise<T>, demo: () => T): Promise<T> {
  if (demoMode) return demo()
  try {
    return await live()
  } catch {
    void probeBackend()
    return demo()
  }
}

// ── Recall (RSE) ──────────────────────────────────────────────────────────────

export function askEcho(query: string, sessionId: string | null): Promise<QueryResponse> {
  return withFallback(
    () =>
      request<QueryResponse>(
        '/retrieval/query',
        { method: 'POST', body: JSON.stringify({ query, session_id: sessionId }) },
        // Cold queries pay one-time model loads (cross-encoder + embedding model) plus
        // 2 Gemini calls and up to 3 widen-loop search rounds — measured ~60s
        // worst case. Aborting early would silently swap in a fabricated demo
        // answer, which is worse than waiting.
        90000,
      ),
    () => demoQueryReply(query),
  )
}

/** The streaming route is missing or refused before sending anything: use the plain POST instead. */
class StreamUnavailableError extends Error {}

/** One server-sent event ("event: x" + "data: {json}" lines) split into its parts. */
function parseSseEvent(block: string): { event: string; data: unknown } | null {
  let event = 'message'
  const dataLines: string[] = []
  for (const line of block.split('\n')) {
    if (line.startsWith('event: ')) event = line.slice(7)
    else if (line.startsWith('data: ')) dataLines.push(line.slice(6))
  }
  return dataLines.length ? { event, data: JSON.parse(dataLines.join('\n')) } : null
}

async function streamQuery(
  query: string,
  sessionId: string | null,
  onStep: (step: QueryStep) => void,
  timeoutMs: number,
): Promise<QueryResponse> {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const response = await fetch(`${API_BASE}/retrieval/query/stream`, {
      method: 'POST',
      signal: controller.signal,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query, session_id: sessionId }),
    })
    if (!response.ok || !response.body) throw new StreamUnavailableError(`${response.status} ${response.statusText}`)

    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      buffer += decoder.decode(value, { stream: true })
      let boundary = buffer.indexOf('\n\n')
      while (boundary !== -1) {
        const parsed = parseSseEvent(buffer.slice(0, boundary))
        buffer = buffer.slice(boundary + 2)
        boundary = buffer.indexOf('\n\n')
        if (!parsed) continue
        if (parsed.event === 'step') onStep(parsed.data as QueryStep)
        else if (parsed.event === 'done') {
          setDemoMode(false)
          return parsed.data as QueryResponse
        } else if (parsed.event === 'error') throw new Error('query failed')
      }
    }
    throw new Error('stream ended without an answer')
  } finally {
    clearTimeout(timer)
  }
}

/**
 * askEcho with live progress: onStep is called as each search step finishes.
 * Falls back to the plain POST when the streaming route is unavailable, and to
 * demo data (like every call here) when the backend is down.
 */
export function askEchoStreaming(
  query: string,
  sessionId: string | null,
  onStep: (step: QueryStep) => void,
): Promise<QueryResponse> {
  const timeoutMs = 90000 // same budget as askEcho: a cold query loads two models
  return withFallback(
    async () => {
      try {
        return await streamQuery(query, sessionId, onStep, timeoutMs)
      } catch (error) {
        if (!(error instanceof StreamUnavailableError)) throw error
        return request<QueryResponse>(
          '/retrieval/query',
          { method: 'POST', body: JSON.stringify({ query, session_id: sessionId }) },
          timeoutMs,
        )
      }
    },
    () => demoQueryReply(query),
  )
}

/** Query string from the given parameters, leaving out the empty ones ("" when none is set). */
function queryString(params: Record<string, string | number | undefined>): string {
  const parts = Object.entries(params)
    .filter(([, value]) => value !== undefined && value !== '')
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`)
  return parts.length ? `?${parts.join('&')}` : ''
}

// ── Wellbeing (WBA) ───────────────────────────────────────────────────────────

/** The past chats, newest first (A27). Empty in demo mode: there is no history to list. */
export function fetchSessions(limit = 50): Promise<SessionSummary[]> {
  return withFallback(() => request<SessionSummary[]>(`/retrieval/sessions?limit=${limit}`), () => [])
}

/**
 * One past chat's messages. Failures are NOT replaced by demo data: the caller
 * must know the chat could not be opened (404 = gone, e.g. removed by the 30-day cleanup).
 */
export async function fetchSessionMessages(sessionId: string): Promise<SessionMessages> {
  if (demoMode) throw new Error('No chat history in demo mode')
  return request<SessionMessages>(`/retrieval/sessions/${encodeURIComponent(sessionId)}`)
}

/**
 * Delete one past chat (D6.2): its questions and replies only, never memories.
 * Like opening a chat, a failure is NOT hidden behind demo data: the caller keeps
 * the chat in the list and says it could not be deleted. A chat that is already
 * gone (404) counts as deleted.
 */
export async function deleteSession(sessionId: string): Promise<void> {
  if (demoMode) throw new Error('No chat history in demo mode')
  try {
    await request<{ messages_deleted: number }>(`/retrieval/sessions/${encodeURIComponent(sessionId)}`, {
      method: 'DELETE',
    })
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) return
    void probeBackend()
    throw error
  }
}

/** One local day's summary (`day` = YYYY-MM-DD; today when left out). */
export function fetchDaily(day?: string): Promise<DailySummary> {
  return withFallback(
    () => request<DailySummary>(`/wellbeing/analytics/daily${queryString({ day })}`),
    () => (day ? { ...demoDaily, day } : demoDaily),
  )
}

/** The hour × weekday grid of the `days` days ending on `endDay` (YYYY-MM-DD; today when left out). */
export function fetchHeatmap(days = 7, endDay?: string): Promise<HeatmapCell[]> {
  return withFallback(
    async () =>
      (await request<{ cells: HeatmapCell[] }>(`/wellbeing/analytics/heatmap${queryString({ days, end_day: endDay })}`))
        .cells,
    () => demoHeatmap,
  )
}

/** The items behind one category's time on `day` (YYYY-MM-DD), longest time first. */
export function fetchCategoryItems(day: string, group: string): Promise<CategoryItem[]> {
  return withFallback(
    async () =>
      (await request<{ items: CategoryItem[] }>(`/wellbeing/analytics/category-items${queryString({ day, group })}`)).items,
    () => demoRecent.slice(0, 4).map((item, index) => ({ ...item, total_seconds: 600 - index * 150 })),
  )
}

export function fetchRecent(limit = 30): Promise<MemoryItem[]> {
  return withFallback(
    async () => (await request<{ items: MemoryItem[] }>(`/wellbeing/recent?limit=${limit}`)).items,
    () => demoRecent,
  )
}

/**
 * One page of the newest-first item list. `before` is the previous page's `next_cursor`.
 * Unlike the first page, an older page that fails is NOT replaced by demo data: it would
 * be appended to the real list, so the caller gets the error and keeps what it shows.
 */
export function fetchRecentPage(limit = 40, before?: string): Promise<RecentPage> {
  return withFallback(
    async () => {
      const page = await request<Partial<RecentPage> & { items: MemoryItem[] }>(
        `/wellbeing/recent${queryString({ limit, before })}`,
      )
      // A backend that is not restarted yet sends no cursor: that means "no older page".
      return { items: page.items, next_cursor: page.next_cursor ?? null }
    },
    () => {
      if (before) throw new Error('No older items in demo mode')
      return { items: demoRecent, next_cursor: null }
    },
  )
}

export function fetchGroups(): Promise<UserGroup[]> {
  return withFallback(
    async () => (await request<{ groups: UserGroup[] }>('/wellbeing/groups')).groups,
    () => demoGroups,
  )
}

export function createGroup(name: string, description?: string): Promise<UserGroup> {
  return withFallback(
    () =>
      request<UserGroup>('/wellbeing/groups', {
        method: 'POST',
        body: JSON.stringify({ group_name: name, description: description ?? null }),
      }),
    () => ({
      group_id: `demo-${Date.now()}`,
      group_name: name,
      description: description ?? null,
      member_count: 0,
      auto_assignment_active: false,
    }),
  )
}

/** All rules of a group, paused ones included (the rules screen shows both). */
export function fetchGroupRules(groupId: string): Promise<GroupRule[]> {
  return withFallback(
    async () =>
      (await request<{ rules: GroupRule[] }>(`/wellbeing/groups/${groupId}/rules?include_inactive=true`)).rules,
    () => demoRules[groupId] ?? [],
  )
}

/**
 * Add a rule to a group (D6.3). Like setRuleActive, a live failure is NOT replaced
 * by demo data: the form must not look saved when it was not, so the caller gets
 * the error (an ApiError with the backend's reason for a refused value). In demo
 * mode the rule exists in this browser only.
 */
export async function addRule(groupId: string, ruleType: RuleType, ruleValue: string): Promise<GroupRule> {
  if (demoMode) {
    return {
      rule_id: `demo-${Date.now()}`,
      group_id: groupId,
      rule_type: ruleType,
      rule_value: ruleValue.trim().toLowerCase(),
      is_active: true,
      created_at: new Date().toISOString(),
      match_count: 0,
    }
  }
  try {
    return await request<GroupRule>(`/wellbeing/groups/${groupId}/rules`, {
      method: 'POST',
      body: JSON.stringify({ rule_type: ruleType, rule_value: ruleValue }),
    })
  } catch (error) {
    if (!(error instanceof ApiError)) void probeBackend()
    throw error
  }
}

/** Rules Echo would suggest from patterns its confirmed members share (step 9). None in demo mode. */
export function fetchRuleSuggestions(groupId: string): Promise<RuleSuggestion[]> {
  return withFallback(
    async () =>
      (await request<{ suggestions: RuleSuggestion[] }>(`/wellbeing/groups/${groupId}/rule-suggestions`)).suggestions,
    () => [],
  )
}

/** The group's members, newest assignment first. None in demo mode. */
export function fetchGroupItems(groupId: string, limit = 200): Promise<GroupMember[]> {
  return withFallback(
    async () => (await request<{ items: GroupMember[] }>(`/wellbeing/groups/${groupId}/items?limit=${limit}`)).items,
    () => [],
  )
}

/**
 * Put one item in a group by hand (D6.4). This is the user's own approval, written
 * through `manual_assign` (the one write path into memory_user_groups). A live
 * failure is passed on, never faked; in demo mode nothing is saved.
 */
export async function assignToGroup(groupId: string, memoryId: string): Promise<AssignResult> {
  if (demoMode) {
    return { memory_id: memoryId, group_id: groupId, member_count: 0, confirmed_count: 0, auto_assignment_active: false }
  }
  try {
    return await request<AssignResult>(`/wellbeing/groups/${groupId}/assign`, {
      method: 'POST',
      body: JSON.stringify({ memory_id: memoryId }),
    })
  } catch (error) {
    if (!(error instanceof ApiError)) void probeBackend()
    throw error
  }
}

/**
 * Pause or resume a rule. Unlike the calls above, a failure is NOT answered with
 * demo data: the toggle must not look saved when it was not, so the caller gets
 * the error and puts the switch back.
 */
export async function setRuleActive(groupId: string, ruleId: string, active: boolean): Promise<void> {
  if (demoMode) return
  try {
    await request<{ is_active: boolean }>(`/wellbeing/groups/${groupId}/rules/${ruleId}`, {
      method: 'PATCH',
      body: JSON.stringify({ active }),
    })
  } catch (error) {
    void probeBackend()
    throw error
  }
}

export function fetchReviewQueue(): Promise<GroupSuggestion[]> {
  return withFallback(
    async () => (await request<{ suggestions: GroupSuggestion[] }>('/wellbeing/groups/review')).suggestions,
    () => [],
  )
}

export function decideSuggestion(suggestionId: string, accept: boolean): Promise<{ decision: string }> {
  return withFallback(
    () =>
      request<{ decision: string }>(`/wellbeing/suggestions/${suggestionId}/decision`, {
        method: 'POST',
        body: JSON.stringify({ accept }),
      }),
    () => ({ decision: accept ? 'accepted' : 'rejected' }),
  )
}

export function fetchRegretAnalytics(): Promise<RegretAnalytics> {
  return withFallback(() => request<RegretAnalytics>('/wellbeing/regret/analytics'), () => demoRegret)
}

export function toggleRegret(memoryId: string, note?: string): Promise<{ regretted: boolean }> {
  return withFallback(
    () =>
      request<{ regretted: boolean }>(`/wellbeing/regret/${memoryId}`, {
        method: 'POST',
        body: JSON.stringify({ note: note ?? null }),
      }),
    () => ({ regretted: true }),
  )
}

/** The reflection on the 7 local days ending on `weekEnd` (YYYY-MM-DD; today when left out). */
export function fetchWeeklyInsight(weekEnd?: string): Promise<WeeklyInsight> {
  return withFallback(
    () => request<WeeklyInsight>(`/wellbeing/insights/weekly${queryString({ week_end: weekEnd })}`, undefined, 30000),
    () => demoInsight,
  )
}

export function setReminderSettings(enabled: boolean): Promise<{ reminders_enabled: boolean }> {
  return withFallback(
    () =>
      request<{ reminders_enabled: boolean }>('/wellbeing/regret/reminders/settings', {
        method: 'POST',
        body: JSON.stringify({ enabled }),
      }),
    () => ({ reminders_enabled: enabled }),
  )
}

/** Today's stored sessions (or those of `day`, YYYY-MM-DD); the backend rebuilds today's on every call. */
export function fetchTodaySessions(day?: string): Promise<SessionRow[]> {
  return withFallback(
    async () => (await request<{ sessions: SessionRow[] }>(`/wellbeing/sessions${queryString({ day })}`)).sessions,
    () => [],
  )
}

/** The reminder check. Always null in demo mode: a reminder is the user's own note, never a made-up one. */
export function fetchRegretReminder(sourceType: SourceType, sessionMinutes: number): Promise<RegretReminder | null> {
  return withFallback(
    async () =>
      (
        await request<{ reminder: RegretReminder | null }>(
          `/wellbeing/regret/reminder?source_type=${sourceType}&session_minutes=${Math.floor(sessionMinutes)}`,
        )
      ).reminder,
    () => null,
  )
}

export function snoozeRegretRemindersToday(): Promise<void> {
  return withFallback(
    async () => {
      await request<{ snoozed_today: boolean }>('/wellbeing/regret/reminders/snooze', { method: 'POST' })
    },
    () => undefined,
  )
}

// ── Capture settings (backend /settings — Redis-backed) ──────────────────────

// How long items are kept (A37). The backend deletes older items at its next start.
export type RetentionChoice = 'forever' | '1y' | '6m'

export interface CaptureSettings {
  gmail_enabled: boolean
  chrome_enabled: boolean
  youtube_enabled: boolean
  excluded_domains: string[]
  excluded_senders: string[]
  retention: RetentionChoice
}

const DEMO_CAPTURE_SETTINGS: CaptureSettings = {
  gmail_enabled: true,
  chrome_enabled: true,
  youtube_enabled: true,
  excluded_domains: [],
  excluded_senders: [],
  retention: 'forever',
}

export function fetchCaptureSettings(): Promise<CaptureSettings> {
  return withFallback(() => request<CaptureSettings>('/settings'), () => DEMO_CAPTURE_SETTINGS)
}

export function updateCaptureSettings(update: Partial<CaptureSettings>): Promise<CaptureSettings> {
  return withFallback(
    () => request<CaptureSettings>('/settings', { method: 'POST', body: JSON.stringify(update) }),
    () => ({ ...DEMO_CAPTURE_SETTINGS, ...update }),
  )
}
