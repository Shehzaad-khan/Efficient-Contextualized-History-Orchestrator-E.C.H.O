const STORAGE_KEY = 'echo-regrets'

export const regretSeed = [
  {
    id: 1,
    source: 'Chat',
    category: 'Work',
    title: 'Missed interview follow-up',
    when: 'Today, 9:12 AM',
    detail: 'User wanted to revisit the TechCorp interview email and availability instructions.',
  },
  {
    id: 2,
    source: 'Chat',
    category: 'Study',
    title: 'Forgot article notes',
    when: 'Last week',
    detail: 'User asked about an OS memory management article they wanted to save properly.',
  },
]

export function loadRegrets() {
  if (typeof window === 'undefined') return regretSeed

  try {
    const stored = window.localStorage.getItem(STORAGE_KEY)
    if (!stored) return regretSeed

    const parsed = JSON.parse(stored)
    return Array.isArray(parsed) && parsed.length > 0 ? parsed : regretSeed
  } catch {
    return regretSeed
  }
}

export function saveRegrets(regrets) {
  if (typeof window === 'undefined') return
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(regrets))
}

export function buildRegretEntry(query, source, response) {
  const lower = query.toLowerCase()
  let category = 'General'

  if (lower.includes('work') || lower.includes('job') || lower.includes('interview') || lower.includes('email') || lower.includes('gmail')) {
    category = 'Work'
  } else if (lower.includes('study') || lower.includes('read') || lower.includes('article') || lower.includes('chrome')) {
    category = 'Study'
  } else if (lower.includes('watch') || lower.includes('video') || lower.includes('youtube')) {
    category = 'Entertainment'
  }

  return {
    id: Date.now(),
    source,
    category,
    title: query.length > 60 ? `${query.slice(0, 57)}...` : query,
    when: new Date().toLocaleString(),
    detail: response,
  }
}

export function isRegretQuery(query) {
  const lower = query.toLowerCase()
  return [
    'regret',
    'should not',
    "shouldn't",
    'wish i had',
    'bad decision',
    'mistake',
    'remorse',
    'sorry i did',
    'i feel bad',
  ].some((phrase) => lower.includes(phrase))
}
