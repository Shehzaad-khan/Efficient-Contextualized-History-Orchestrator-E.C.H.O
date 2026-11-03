/** Formatting helpers shared across pages. */

export function formatDuration(totalSeconds: number): string {
  const seconds = Math.max(0, Math.round(totalSeconds))
  const hours = Math.floor(seconds / 3600)
  const minutes = Math.round((seconds % 3600) / 60)
  if (hours === 0) return `${minutes}m`
  return `${hours}h ${String(minutes).padStart(2, '0')}m`
}

/** The backend sends naive UTC timestamps (no zone); read them as UTC, not local time. */
export function asUtc(iso: string | null | undefined): string | null | undefined {
  if (!iso) return iso
  return /[zZ]|[+-]\d\d:?\d\d$/.test(iso) ? iso : `${iso}Z`
}

export function formatWhen(iso: string | null | undefined): string {
  if (!iso) return 'unknown time'
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return 'unknown time'

  const now = new Date()
  const diffMs = now.getTime() - date.getTime()
  const diffHours = diffMs / 3.6e6

  const time = date.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' })
  if (diffHours < 24 && date.getDate() === now.getDate()) return `today · ${time}`
  const yesterday = new Date(now)
  yesterday.setDate(now.getDate() - 1)
  if (date.getDate() === yesterday.getDate() && diffHours < 48) return `yesterday · ${time}`
  return `${date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })} · ${time}`
}

export function dayLabel(iso: string | null | undefined): string {
  if (!iso) return 'Undated'
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return 'Undated'
  const now = new Date()
  if (date.toDateString() === now.toDateString()) return 'Today'
  const yesterday = new Date(now)
  yesterday.setDate(now.getDate() - 1)
  if (date.toDateString() === yesterday.toDateString()) return 'Yesterday'
  return date.toLocaleDateString(undefined, { weekday: 'long', month: 'long', day: 'numeric' })
}

/** A Date's LOCAL calendar day as YYYY-MM-DD (the backend's `day` parameters are local days). */
export function toLocalIsoDate(date: Date): string {
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${date.getFullYear()}-${month}-${day}`
}

/** Today's local day as YYYY-MM-DD. */
export function todayIso(): string {
  return toLocalIsoDate(new Date())
}

/** A YYYY-MM-DD day moved by `days` (negative = back), on the local calendar. */
export function shiftIsoDate(iso: string, days: number): string {
  const [year, month, day] = iso.split('-').map(Number)
  return toLocalIsoDate(new Date(year, month - 1, day + days))
}

/** A YYYY-MM-DD day for a heading: "today", "yesterday", or e.g. "Thu 29 Oct". */
export function isoDayLabel(iso: string): string {
  const today = todayIso()
  if (iso === today) return 'today'
  if (iso === shiftIsoDate(today, -1)) return 'yesterday'
  const [year, month, day] = iso.split('-').map(Number)
  return new Date(year, month - 1, day).toLocaleDateString(undefined, { weekday: 'short', day: 'numeric', month: 'short' })
}

export const HOUR_LABELS = ['12a', '', '', '3a', '', '', '6a', '', '', '9a', '', '', '12p', '', '', '3p', '', '', '6p', '', '', '9p', '', '']
export const DOW_LABELS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
