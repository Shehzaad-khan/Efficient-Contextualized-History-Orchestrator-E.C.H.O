import type { RuleType } from './types'

/** How one kind of rule is named and explained to someone who has never heard "domain" or "sender". */
export interface RuleKind {
  /** The choice in the "what should Echo look for?" list. */
  label: string
  /** A few words naming the kind next to a saved rule. */
  short: string
  placeholder: string
  hint: string
}

/** The kinds in the order they are offered: what people use most first. */
export const RULE_TYPES: RuleType[] = ['domain', 'sender', 'channel', 'keyword', 'time_window']

export const RULE_KINDS: Record<RuleType, RuleKind> = {
  domain: {
    label: 'A website',
    short: 'website',
    placeholder: 'e.g. leetcode.com',
    hint: 'Matches pages from this website and its sub-sites (gist.github.com counts as github.com).',
  },
  sender: {
    label: 'An email sender',
    short: 'email sender',
    placeholder: 'e.g. pes.edu or hr@acme.com',
    hint: 'A website name like pes.edu matches every address at it; a full address matches only that person.',
  },
  channel: {
    label: 'A YouTube channel',
    short: 'YouTube channel',
    placeholder: 'e.g. Neso Academy',
    hint: 'Type the channel name exactly as YouTube shows it. Matches every video from that channel.',
  },
  keyword: {
    label: 'A word in the title',
    short: 'word in title',
    placeholder: 'e.g. dbms',
    hint: 'Matches any page, email or video whose title contains this word.',
  },
  time_window: {
    label: 'A date range',
    short: 'date range',
    placeholder: '',
    hint: 'Matches items from between these two days, both included.',
  },
}

/** A rule as one plain sentence fragment, e.g. "pages from github.com". */
export function ruleSentence(type: RuleType, value: string): string {
  switch (type) {
    case 'domain':
      return `pages from ${value}`
    case 'sender':
      return `emails from ${value}`
    case 'channel':
      return `videos from ${value}`
    case 'keyword':
      return `titles containing “${value}”`
    case 'time_window': {
      const [start, end] = value.split(',')
      return `items from ${start} to ${end}`
    }
  }
}

/** The same value the backend stores, so two spellings of one rule count once ("WWW.GitHub.com" = "github.com"). */
export function ruleKey(type: RuleType, value: string): string {
  let clean = value.trim().toLowerCase()
  if (type === 'sender') clean = clean.replace(/^@+/, '')
  if (type === 'domain' && clean.startsWith('www.')) clean = clean.slice(4)
  return `${type}:${clean}`
}
