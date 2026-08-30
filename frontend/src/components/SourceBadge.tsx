import type { SourceType } from '../lib/types'

const SOURCE_STYLES: Record<SourceType, { dot: string; label: string }> = {
  gmail: { dot: 'bg-gmail', label: 'GMAIL' },
  chrome: { dot: 'bg-chrome', label: 'CHROME' },
  youtube: { dot: 'bg-youtube', label: 'YOUTUBE' },
}

export default function SourceBadge({ source }: { source: SourceType }) {
  const style = SOURCE_STYLES[source] ?? SOURCE_STYLES.chrome
  return (
    <span className="inline-flex items-center gap-1.5">
      <span aria-hidden="true" className={`h-1.5 w-1.5 rounded-full ${style.dot}`} />
      <span className="meta">{style.label}</span>
    </span>
  )
}
