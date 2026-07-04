export const categorySeed = [
  { name: 'Work', minutes: 180 },
  { name: 'Study', minutes: 95 },
  { name: 'Entertainment', minutes: 70 },
  { name: 'Interview', minutes: 40 },
  { name: 'Sports', minutes: 25 },
]

export const activityItems = [
  {
    id: 1,
    category: 'Work',
    source: 'Gmail',
    title: 'TechCorp interview follow-up',
    when: 'Today, 9:12 AM',
    detail: 'Recruiter sent next-step instructions and asked for availability.',
  },
  {
    id: 2,
    category: 'Study',
    source: 'Chrome',
    title: 'OS memory management article',
    when: 'Last week',
    detail: 'Article comparing paging, segmentation, and memory allocation.',
  },
  {
    id: 3,
    category: 'Entertainment',
    source: 'YouTube',
    title: 'Project planning video',
    when: 'Two days ago',
    detail: 'Planning milestones, dependencies, and delivery checkpoints.',
  },
  {
    id: 4,
    category: 'Interview',
    source: 'Gmail',
    title: 'Interview prep checklist',
    when: 'Yesterday',
    detail: 'Questions, reminders, and notes before the next interview round.',
  },
  {
    id: 5,
    category: 'Sports',
    source: 'Chrome',
    title: 'Workout scheduling note',
    when: 'Earlier today',
    detail: 'A quick lookup for training routine and rest-day planning.',
  },
]

export const chatHistorySeed = [
  {
    id: 101,
    prompt: 'Show only the Gmail interview email',
    response: 'I found a Gmail match: TechCorp interview follow-up with availability instructions.',
    source: 'Gmail',
  },
  {
    id: 102,
    prompt: 'Find the YouTube video I watched about planning',
    response: 'I found a YouTube match: a project planning video about milestones and delivery checkpoints.',
    source: 'YouTube',
  },
  {
    id: 103,
    prompt: 'What did I read on Chrome after the email?',
    response: 'I found a Chrome match: an OS memory management article you opened after the interview email.',
    source: 'Chrome',
  },
]
