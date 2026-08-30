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
} from './demoData'
import type {
  DailySummary,
  GroupSuggestion,
  HeatmapCell,
  MemoryItem,
  QueryResponse,
  RegretAnalytics,
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
    if (!response.ok) throw new Error(`${response.status} ${response.statusText}`)
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
        // Cold queries pay one-time model loads (cross-encoder + MiniLM) plus
        // 2 Gemini calls and up to 3 widen-loop search rounds — measured ~60s
        // worst case. Aborting early would silently swap in a fabricated demo
        // answer, which is worse than waiting.
        90000,
      ),
    () => demoQueryReply(query),
  )
}

// ── Wellbeing (WBA) ───────────────────────────────────────────────────────────

export function fetchDaily(): Promise<DailySummary> {
  return withFallback(() => request<DailySummary>('/wellbeing/analytics/daily'), () => demoDaily)
}

export function fetchHeatmap(days = 7): Promise<HeatmapCell[]> {
  return withFallback(
    async () => (await request<{ cells: HeatmapCell[] }>(`/wellbeing/analytics/heatmap?days=${days}`)).cells,
    () => demoHeatmap,
  )
}

export function fetchRecent(limit = 30): Promise<MemoryItem[]> {
  return withFallback(
    async () => (await request<{ items: MemoryItem[] }>(`/wellbeing/recent?limit=${limit}`)).items,
    () => demoRecent,
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

export function fetchWeeklyInsight(): Promise<WeeklyInsight> {
  return withFallback(() => request<WeeklyInsight>('/wellbeing/insights/weekly', undefined, 30000), () => demoInsight)
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

// ── Capture settings (backend /settings — Redis-backed) ──────────────────────

export interface CaptureSettings {
  gmail_enabled: boolean
  chrome_enabled: boolean
  youtube_enabled: boolean
  excluded_domains: string[]
  excluded_senders: string[]
}

const DEMO_CAPTURE_SETTINGS: CaptureSettings = {
  gmail_enabled: true,
  chrome_enabled: true,
  youtube_enabled: true,
  excluded_domains: [],
  excluded_senders: [],
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
