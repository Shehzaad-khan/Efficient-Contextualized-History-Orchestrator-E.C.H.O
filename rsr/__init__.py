"""
RSE (Retrieval & Synthesis Engine) - ECHO's Intelligence Layer

This module implements the complete retrieval and synthesis pipeline for answering
user queries over their unified digital history.

ARCHITECTURE:

1. Search Coordination (search_coordinator.py)
   - Blends SQL keyword search with FAISS vector search
   - Implements fallback behavior for weak results
   - Returns ranked candidate results

2. Retrieval Graph (retrieval_graph.py)
   - LangGraph-based orchestration of retrieval workflow
   - Conditional routing based on result quality
   - Multi-turn conversation support

3. Reranking (reranker.py)
   - Scores results using semantic similarity + engagement + recency
   - Filters low-quality results
   - Deduplicates and diversifies results

4. Synthesis (synthesizer.py)
   - Synthesizes final answer from top results
   - LLM-based synthesis (Claude/Gemini) or template-based
   - Includes proper citations to source materials

5. Quality Evaluation (quality_evaluator.py)
   - Assesses result relevance, coverage, diversity
   - Tracks answer quality metrics
   - Provides feedback for system improvements

6. Quality Test Set (query_quality_eval_set.py)
   - 10 benchmark queries covering different retrieval scenarios
   - Expected results and ground truth answers
   - Difficulty-based quality benchmarks

USAGE:

    from rse.retrieval_graph import RetrievalExecutor
    from rse.quality_evaluator import QualityEvaluator
    
    executor = RetrievalExecutor()
    answer = executor.execute("Show me work emails from March")
    
    evaluator = QualityEvaluator()
    quality = evaluator.evaluate_answer(answer, results, query)

QUALITY METRICS:

- Relevance (0-1): How well results match query intent
- Coverage (0-1): How thoroughly results cover the topic
- Diversity (0-1): How varied the results are
- Confidence (0-1): System's confidence in result quality
- Overall Quality: strong/good/fair/poor

MODULE DEPENDENCIES:

- LangGraph: Graph-based workflow orchestration
- LangChain: Message history and LLM integration
- PostgreSQL: Keyword search and metadata
- FAISS: Vector similarity search
- sentence-transformers: Embedding generation (from ENP)
- Claude/Gemini API: LLM synthesis

TEAMMATE 3 RESPONSIBILITIES:

✓ Finalize retrieval graph behavior (routing, search coordination)
✓ Tighten SQL + vector retrieval blend
✓ Implement fallback behavior
✓ Define quality eval set
✓ Track hit quality and improve answer relevance
✓ Ensure deterministic retrieval behavior
"""

from retrival.search_coordinator import SearchCoordinator
from retrival.retrieval_graph import RetrievalExecutor, build_retrieval_graph
from retrival.reranker import Reranker, ContextualReranker
from retrival.synthesizer import Synthesizer
from retrival.quality_evaluator import QualityEvaluator
from retrival.query_quality_eval_set import (
    QUALITY_EVAL_SET,
    QUALITY_BENCHMARKS,
    run_quality_evaluation,
    get_eval_query
)

__all__ = [
    "SearchCoordinator",
    "RetrievalExecutor",
    "build_retrieval_graph",
    "Reranker",
    "ContextualReranker",
    "Synthesizer",
    "QualityEvaluator",
    "QUALITY_EVAL_SET",
    "QUALITY_BENCHMARKS",
    "run_quality_evaluation",
    "get_eval_query"
]

__version__ = "1.0.0"
__author__ = "Teammate 3 - Retrieval Intelligence Owner"
