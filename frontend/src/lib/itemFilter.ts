import type { MemoryItem } from './types'

/** The site, sender or channel an item came from: the second line of an item row. */
export function itemLocation(item: MemoryItem): string {
  if (item.source_type === 'gmail') return item.sender ?? ''
  if (item.source_type === 'chrome') return item.domain ?? ''
  return item.channel_name ?? ''
}

/** Items whose title or sender / site / channel contains every word typed (case does not matter). */
export function filterItems(items: MemoryItem[], search: string): MemoryItem[] {
  const words = search.toLowerCase().split(/\s+/).filter(Boolean)
  if (words.length === 0) return items
  return items.filter((item) => {
    const haystack = `${item.title ?? ''} ${itemLocation(item)}`.toLowerCase()
    return words.every((word) => haystack.includes(word))
  })
}
