"""
Quality Evaluation Test Set

Defines benchmark queries and expected relevance for testing retrieval quality.
Used for:
- Validating retrieval accuracy
- Tracking quality improvements
- A/B testing different retrieval strategies

QUALITY EVAL SET STRUCTURE:
- Each query has expected_results (what should be retrieved)
- Expected sources (which sources should dominate)
- Expected system_groups (categorization)
- Ground truth answer
"""

from typing import List, Dict, Any
from datetime import datetime, timedelta

# ========================
# QUALITY EVAL QUERIES
# ========================

QUALITY_EVAL_SET: List[Dict[str, Any]] = [
    # ==================
    # Query 1: Work-focused
    # ==================
    {
        "id": "eval_001",
        "query": "Show me all the project deadlines and deliverables from March",
        "category": "work_retrieval",
        "expected_sources": ["gmail", "chrome"],
        "expected_system_groups": [1],  # Work only
        "expected_domains": ["jira.com", "asana.com", "github.com", "outlook.office.com"],
        "keywords": ["deadline", "deliverable", "sprint", "project"],
        "expected_result_count_min": 3,
        "expected_result_count_max": 20,
        "ground_truth_answer": "You had project deadlines for multiple deliverables in March, including the Q1 API refactor due 3/15 and the dashboard redesign due 3/28.",
        "difficulty": "medium"
    },
    
    # ==================
    # Query 2: Learning-focused
    # ==================
    {
        "id": "eval_002",
        "query": "What Python concepts did I study last week?",
        "category": "study_retrieval",
        "expected_sources": ["chrome", "youtube"],
        "expected_system_groups": [2],  # Study only
        "expected_domains": ["stackoverflow.com", "geeksforgeeks.org", "youtube.com", "coursera.org"],
        "keywords": ["python", "tutorial", "lecture", "concept"],
        "expected_result_count_min": 2,
        "expected_result_count_max": 15,
        "ground_truth_answer": "You studied decorators, generators, and context managers. You watched a tutorial on decorators and read multiple Stack Overflow answers about context managers.",
        "difficulty": "medium"
    },
    
    # ==================
    # Query 3: Time-filtered
    # ==================
    {
        "id": "eval_003",
        "query": "Show me emails from today",
        "category": "time_filtering",
        "expected_sources": ["gmail"],
        "expected_system_groups": [1, 4, 5],  # Work, personal, misc
        "keywords": ["email"],
        "expected_result_count_min": 1,
        "expected_result_count_max": 50,
        "time_filter": "today",
        "ground_truth_answer": "You received 15 emails today from various senders including your boss, colleagues, and newsletters.",
        "difficulty": "easy"
    },
    
    # ==================
    # Query 4: Multi-turn context
    # ==================
    {
        "id": "eval_004",
        "query": "Show me more about that topic",
        "context": [
            "I was reading about machine learning applications",
            "Specifically about NLP and natural language processing"
        ],
        "category": "multi_turn",
        "expected_sources": ["chrome", "youtube"],
        "expected_system_groups": [2],  # Study
        "keywords": ["machine learning", "NLP", "natural language processing"],
        "expected_result_count_min": 5,
        "expected_result_count_max": 30,
        "ground_truth_answer": "You have several resources on NLP including a Stanford course video, a research paper on BERT, and a Medium article on transformer models.",
        "difficulty": "hard"
    },
    
    # ==================
    # Query 5: Entertainment
    # ==================
    {
        "id": "eval_005",
        "query": "What movies and shows have I watched recently?",
        "category": "entertainment_retrieval",
        "expected_sources": ["youtube"],
        "expected_system_groups": [3],  # Entertainment
        "expected_domains": ["youtube.com", "netflix.com"],
        "keywords": ["movie", "show", "watch", "film"],
        "expected_result_count_min": 3,
        "expected_result_count_max": 20,
        "ground_truth_answer": "In the last month you watched Breaking Bad (episodes 5-8), The Office (rewatched episode 3), and several YouTube film reviews about Marvel movies.",
        "difficulty": "medium"
    },
    
    # ==================
    # Query 6: Cross-source
    # ==================
    {
        "id": "eval_006",
        "query": "Find all information about the product launch we discussed",
        "category": "cross_source",
        "expected_sources": ["gmail", "chrome", "youtube"],
        "expected_system_groups": [1, 2],  # Work and study
        "keywords": ["product", "launch", "release"],
        "expected_result_count_min": 5,
        "expected_result_count_max": 25,
        "ground_truth_answer": "You received 3 emails about the launch timeline, visited 2 wiki pages with product specs, and watched a launch strategy presentation video.",
        "difficulty": "hard"
    },
    
    # ==================
    # Query 7: Personal
    # ==================
    {
        "id": "eval_007",
        "query": "Show me messages from friends about the weekend plans",
        "category": "personal_retrieval",
        "expected_sources": ["gmail"],
        "expected_system_groups": [4],  # Personal
        "keywords": ["friend", "weekend", "plans", "hang out"],
        "expected_result_count_min": 2,
        "expected_result_count_max": 10,
        "ground_truth_answer": "You have emails from 3 friends about weekend hiking plans and a group chat about meeting up Saturday.",
        "difficulty": "medium"
    },
    
    # ==================
    # Query 8: Ambiguous
    # ==================
    {
        "id": "eval_008",
        "query": "GitHub",
        "category": "short_ambiguous",
        "expected_sources": ["chrome", "gmail"],
        "expected_system_groups": [1, 2],  # Work or study
        "keywords": ["github"],
        "expected_result_count_min": 5,
        "expected_result_count_max": 30,
        "ground_truth_answer": "GitHub related items: work commits, personal projects, tutorials you read, and notifications.",
        "difficulty": "medium"
    },
    
    # ==================
    # Query 9: Specific entity
    # ==================
    {
        "id": "eval_009",
        "query": "What did I research about AWS?",
        "category": "specific_entity",
        "expected_sources": ["chrome", "youtube"],
        "expected_system_groups": [1, 2],  # Work and study
        "expected_domains": ["aws.amazon.com", "youtube.com", "reddit.com"],
        "keywords": ["AWS", "cloud", "lambda", "EC2"],
        "expected_result_count_min": 3,
        "expected_result_count_max": 15,
        "ground_truth_answer": "You viewed AWS documentation on Lambda, watched a deployment tutorial, and read a Reddit discussion about cost optimization.",
        "difficulty": "medium"
    },
    
    # ==================
    # Query 10: Negative case
    # ==================
    {
        "id": "eval_010",
        "query": "Show me information about ancient Roman history",
        "category": "negative_case",
        "expected_sources": [],
        "expected_system_groups": [],
        "expected_result_count_min": 0,
        "expected_result_count_max": 3,
        "ground_truth_answer": "No results found. You don't appear to have any digital history related to Roman history.",
        "difficulty": "easy"
    },
]


# ========================
# QUALITY BENCHMARKS
# ========================

QUALITY_BENCHMARKS = {
    "easy": {
        "min_precision": 0.8,  # 80% of top 5 should be relevant
        "min_recall": 0.7,     # Should find at least 70% of available relevant items
        "min_relevance": 0.75,
        "min_coverage": 0.6
    },
    "medium": {
        "min_precision": 0.7,
        "min_recall": 0.6,
        "min_relevance": 0.65,
        "min_coverage": 0.5
    },
    "hard": {
        "min_precision": 0.5,
        "min_recall": 0.4,
        "min_relevance": 0.5,
        "min_coverage": 0.4
    }
}


# ========================
# SAMPLE DATA FOR TESTING
# ========================

SAMPLE_RETRIEVED_RESULTS = {
    "eval_001": [
        {
            "memory_id": "mem_001",
            "title": "Q1 Project Deadlines",
            "source_type": "gmail",
            "text": "Project deliverables for Q1 2026: API refactor due 3/15, dashboard redesign due 3/28",
            "system_group_id": 1,
            "created_at": "2026-03-01T10:00:00Z",
            "engagement_score": 85,
            "rerank_score": 0.95
        },
        {
            "memory_id": "mem_002",
            "title": "Sprint Planning - March",
            "source_type": "chrome",
            "text": "Sprint goals and deliverables for March. Backend improvements and UI fixes.",
            "system_group_id": 1,
            "created_at": "2026-03-05T14:30:00Z",
            "engagement_score": 75,
            "rerank_score": 0.87
        },
        {
            "memory_id": "mem_003",
            "title": "Jira - March Sprint Board",
            "source_type": "chrome",
            "text": "All tasks and deadlines organized by priority and assignee",
            "system_group_id": 1,
            "created_at": "2026-03-01T08:00:00Z",
            "engagement_score": 92,
            "rerank_score": 0.91
        }
    ],
    
    "eval_002": [
        {
            "memory_id": "mem_101",
            "title": "Python Decorators Tutorial - Real Python",
            "source_type": "chrome",
            "text": "Complete guide to decorators in Python. Function wrapping, parameters, stacking decorators.",
            "system_group_id": 2,
            "created_at": "2026-06-06T12:00:00Z",
            "engagement_score": 88,
            "rerank_score": 0.92
        },
        {
            "memory_id": "mem_102",
            "title": "Understanding Python Generators and Context Managers",
            "source_type": "youtube",
            "text": "Tutorial video explaining generators and context managers in depth",
            "system_group_id": 2,
            "created_at": "2026-06-05T15:00:00Z",
            "engagement_score": 85,
            "rerank_score": 0.89
        },
        {
            "memory_id": "mem_103",
            "title": "Stack Overflow - Context Manager Question",
            "source_type": "chrome",
            "text": "Discussion about context managers and their practical applications",
            "system_group_id": 2,
            "created_at": "2026-06-04T09:30:00Z",
            "engagement_score": 72,
            "rerank_score": 0.81
        }
    ]
}


# ========================
# EVALUATION FUNCTIONS
# ========================

def get_eval_query(query_id: str) -> Dict[str, Any]:
    """Get evaluation query by ID."""
    for q in QUALITY_EVAL_SET:
        if q["id"] == query_id:
            return q
    return None


def get_benchmarks(difficulty: str) -> Dict[str, float]:
    """Get quality benchmarks for difficulty level."""
    return QUALITY_BENCHMARKS.get(difficulty, QUALITY_BENCHMARKS["medium"])


def get_sample_results(query_id: str) -> List[Dict[str, Any]]:
    """Get sample results for testing."""
    return SAMPLE_RETRIEVED_RESULTS.get(query_id, [])


def run_quality_evaluation(
    query_id: str,
    actual_results: List[Dict[str, Any]],
    quality_evaluator
) -> Dict[str, Any]:
    """
    Run quality evaluation for a query.
    
    Args:
        query_id: ID of evaluation query
        actual_results: Results returned by retrieval system
        quality_evaluator: QualityEvaluator instance
        
    Returns:
        Evaluation report
    """
    
    query = get_eval_query(query_id)
    if not query:
        return {"error": f"Query {query_id} not found"}
    
    benchmarks = get_benchmarks(query["difficulty"])
    
    # Evaluate
    evaluation = quality_evaluator.evaluate_results(
        actual_results,
        query["query"],
        expected_sources=query.get("expected_sources"),
        expected_domains=query.get("expected_domains")
    )
    
    # Compare to benchmarks
    passed = all([
        evaluation["relevance"] >= benchmarks["min_relevance"],
        evaluation["coverage"] >= benchmarks["min_coverage"]
    ])
    
    return {
        "query_id": query_id,
        "query": query["query"],
        "difficulty": query["difficulty"],
        "evaluation": evaluation,
        "benchmarks": benchmarks,
        "passed": passed,
        "timestamp": datetime.now().isoformat()
    }
