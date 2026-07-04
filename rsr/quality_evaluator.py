"""
Quality Evaluator - Evaluates retrieval quality and answer relevance

Responsibilities:
- Assesses result relevance to query
- Computes quality metrics (precision, coverage)
- Tracks answer quality over time
- Provides feedback for reranking improvements

QUALITY METRICS:
1. Relevance Score (0-1): How well results match query intent
2. Coverage (0-1): How thoroughly results cover the topic
3. Diversity (0-1): How varied the results are
4. Confidence (0-1): How sure we are of the answer quality
"""

import logging
from typing import List, Dict, Any, Tuple
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class QualityEvaluator:
    """Evaluates retrieval and synthesis quality."""
    
    def __init__(self):
        self.evaluation_history = []
    
    def evaluate_results(
        self,
        results: List[Dict[str, Any]],
        query: str,
        expected_sources: List[str] = None,
        expected_domains: List[str] = None
    ) -> Dict[str, Any]:
        """
        Evaluate quality of retrieved results.
        
        Args:
            results: Retrieved and ranked results
            query: User query
            expected_sources: Expected source types (for testing)
            expected_domains: Expected domains (for testing)
            
        Returns:
            Quality assessment dict
        """
        
        if not results:
            return {
                "relevance": 0.0,
                "coverage": 0.0,
                "diversity": 0.0,
                "overall_quality": "poor",
                "confidence": 0.0,
                "metrics": {}
            }
        
        # Calculate individual metrics
        relevance = self._calculate_relevance(results, query)
        coverage = self._calculate_coverage(results)
        diversity = self._calculate_diversity(results)
        confidence = self._calculate_confidence(results, relevance)
        
        # Overall quality assessment
        overall = self._assess_overall_quality(relevance, coverage, diversity)
        
        # Build evaluation record
        evaluation = {
            "query": query,
            "result_count": len(results),
            "relevance": relevance,
            "coverage": coverage,
            "diversity": diversity,
            "confidence": confidence,
            "overall_quality": overall,
            "timestamp": datetime.now().isoformat(),
            "metrics": {
                "avg_rerank_score": sum(r.get("rerank_score", 0) for r in results) / len(results),
                "top_score": results[0].get("rerank_score", 0),
                "score_variance": self._calculate_variance([r.get("rerank_score", 0) for r in results])
            }
        }
        
        # Store for tracking
        self.evaluation_history.append(evaluation)
        
        logger.info(f"Quality eval: {overall} (relevance: {relevance:.2f}, coverage: {coverage:.2f})")
        
        return evaluation
    
    def evaluate_answer(
        self,
        answer: str,
        results: List[Dict[str, Any]],
        query: str,
        ground_truth: str = None
    ) -> Dict[str, Any]:
        """
        Evaluate quality of synthesized answer.
        
        Args:
            answer: Synthesized answer text
            results: Source results used
            query: Original query
            ground_truth: Expected correct answer (for testing)
            
        Returns:
            Answer quality assessment
        """
        
        # Calculate metrics
        answer_length = len(answer.split())
        has_citations = self._check_citations_present(answer)
        cite_count = self._count_citations(answer)
        coherence = self._evaluate_coherence(answer, query)
        
        # Compare to ground truth if provided
        accuracy = 0.0
        if ground_truth:
            accuracy = self._compare_to_ground_truth(answer, ground_truth)
        
        overall_quality = self._assess_answer_quality(
            has_citations,
            coherence,
            accuracy,
            cite_count,
            len(results)
        )
        
        evaluation = {
            "answer": answer[:200],  # Store snippet for reference
            "query": query,
            "metrics": {
                "length": answer_length,
                "has_citations": has_citations,
                "citation_count": cite_count,
                "coherence": coherence,
                "accuracy": accuracy
            },
            "overall_quality": overall_quality,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"Answer quality: {overall_quality} (coherence: {coherence:.2f})")
        
        return evaluation
    
    # ========================
    # RESULT QUALITY METRICS
    # ========================
    
    def _calculate_relevance(
        self,
        results: List[Dict[str, Any]],
        query: str
    ) -> float:
        """
        Calculate relevance of results to query.
        
        Based on:
        - Average rerank score of top 5
        - Score distribution (high variance = relevance inconsistency)
        """
        
        if not results:
            return 0.0
        
        # Top 5 scores
        top_scores = [r.get("rerank_score", 0) for r in results[:5]]
        
        # Average normalized to 0-1
        avg_score = sum(top_scores) / len(top_scores)
        
        return min(avg_score, 1.0)
    
    def _calculate_coverage(self, results: List[Dict[str, Any]]) -> float:
        """
        Calculate coverage - how well results cover different aspects.
        
        Based on:
        - Diversity of sources and domains
        - Number of results
        - System group diversity
        """
        
        if len(results) < 3:
            return max(0.0, len(results) / 3.0 * 0.5)  # Max 0.5 for <3 results
        
        # Source diversity
        sources = set(r.get("source_type") for r in results[:10])
        source_diversity = len(sources) / 3.0  # 3 sources max
        
        # System group diversity
        groups = set(r.get("system_group_id") for r in results[:10])
        group_diversity = min(len(groups) / 3.0, 1.0)  # Normalize
        
        # Result count coverage
        result_coverage = min(len(results) / 10.0, 1.0)  # 10+ results = full coverage
        
        # Combine
        coverage = (source_diversity * 0.4 + group_diversity * 0.3 + result_coverage * 0.3)
        
        return min(coverage, 1.0)
    
    def _calculate_diversity(self, results: List[Dict[str, Any]]) -> float:
        """
        Calculate diversity - how varied the results are.
        
        High diversity = results from different sources, domains, dates
        Low diversity = all similar results (possibly duplicates)
        """
        
        if len(results) < 2:
            return 0.0
        
        # Source distribution
        sources = {}
        domains = set()
        dates = set()
        
        for r in results[:20]:
            source = r.get("source_type", "unknown")
            sources[source] = sources.get(source, 0) + 1
            
            domain = r.get("metadata", {}).get("domain", "")
            if domain:
                domains.add(domain)
            
            date = r.get("created_at", "").split("T")[0]  # Date only
            if date:
                dates.add(date)
        
        # Source balance (1.0 = evenly distributed)
        source_counts = list(sources.values())
        if source_counts:
            source_balance = 1.0 - (max(source_counts) - min(source_counts)) / max(source_counts)
        else:
            source_balance = 0.5
        
        # Domain variety
        domain_diversity = len(domains) / max(10, len(results))
        
        # Temporal spread
        date_diversity = len(dates) / max(5, len(results))
        
        # Combine
        diversity = (source_balance * 0.4 + domain_diversity * 0.3 + date_diversity * 0.3)
        
        return min(diversity, 1.0)
    
    def _calculate_confidence(
        self,
        results: List[Dict[str, Any]],
        relevance: float
    ) -> float:
        """
        Calculate confidence in result quality.
        
        Based on:
        - Relevance score
        - Score consistency (low variance = high confidence)
        - Number of results
        """
        
        if not results:
            return 0.0
        
        # Score consistency
        scores = [r.get("rerank_score", 0) for r in results[:5]]
        variance = self._calculate_variance(scores)
        consistency = 1.0 - min(variance, 1.0)  # High variance = low consistency
        
        # Result count
        count_confidence = min(len(results) / 5.0, 1.0)
        
        # Combine
        confidence = (relevance * 0.5 + consistency * 0.3 + count_confidence * 0.2)
        
        return min(confidence, 1.0)
    
    def _assess_overall_quality(
        self,
        relevance: float,
        coverage: float,
        diversity: float
    ) -> str:
        """
        Assess overall quality (strong/good/fair/poor).
        """
        
        avg_metric = (relevance + coverage + diversity) / 3.0
        
        if avg_metric >= 0.75:
            return "strong"
        elif avg_metric >= 0.5:
            return "good"
        elif avg_metric >= 0.25:
            return "fair"
        else:
            return "poor"
    
    # ========================
    # ANSWER QUALITY METRICS
    # ========================
    
    def _check_citations_present(self, answer: str) -> bool:
        """Check if answer includes citations."""
        return "source" in answer.lower() or "from" in answer.lower()
    
    def _count_citations(self, answer: str) -> int:
        """Count number of citations in answer."""
        # Simple count: number of [ markers or "from" mentions
        return answer.count("[") + answer.count("from ")
    
    def _evaluate_coherence(self, answer: str, query: str) -> float:
        """
        Evaluate answer coherence (how well it stays on topic).
        
        Based on:
        - Presence of query keywords in answer
        - Answer length (too short = incomplete)
        - Sentence structure
        """
        
        answer_lower = answer.lower()
        query_lower = query.lower()
        
        # Check for query keyword presence
        keywords = query_lower.split()
        keyword_matches = sum(1 for kw in keywords if kw in answer_lower)
        keyword_score = keyword_matches / max(len(keywords), 1)
        
        # Check length (good answers are 2-8 sentences)
        sentence_count = answer.count('.') + answer.count('?') + answer.count('!')
        length_score = 1.0 if 2 <= sentence_count <= 8 else 0.7
        
        coherence = (keyword_score * 0.6 + length_score * 0.4)
        
        return min(coherence, 1.0)
    
    def _compare_to_ground_truth(self, answer: str, ground_truth: str) -> float:
        """
        Compare answer to ground truth (for evaluation set).
        
        Simple: check if answer contains key concepts from ground truth.
        """
        
        # Extract keywords from ground truth
        truth_keywords = set(ground_truth.lower().split())
        answer_words = set(answer.lower().split())
        
        # Calculate overlap
        matches = len(truth_keywords & answer_words)
        accuracy = matches / len(truth_keywords)
        
        return min(accuracy, 1.0)
    
    def _assess_answer_quality(
        self,
        has_citations: bool,
        coherence: float,
        accuracy: float,
        cite_count: int,
        result_count: int
    ) -> str:
        """
        Assess answer quality (excellent/good/fair/poor).
        """
        
        score = 0.0
        
        # Citations bonus
        score += 0.3 if has_citations else 0.0
        score += 0.1 if cite_count >= 2 else 0.0
        
        # Coherence
        score += coherence * 0.3
        
        # Accuracy
        score += accuracy * 0.2
        
        # Results used
        score += 0.1 if result_count >= 3 else 0.05
        
        if score >= 0.75:
            return "excellent"
        elif score >= 0.6:
            return "good"
        elif score >= 0.4:
            return "fair"
        else:
            return "poor"
    
    # ========================
    # UTILITIES
    # ========================
    
    def _calculate_variance(self, values: List[float]) -> float:
        """Calculate variance of values."""
        if len(values) < 2:
            return 0.0
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        
        return variance
    
    def get_quality_report(self) -> Dict[str, Any]:
        """Generate quality report from history."""
        
        if not self.evaluation_history:
            return {"error": "No evaluations yet"}
        
        # Calculate aggregates
        total_evals = len(self.evaluation_history)
        avg_relevance = sum(e.get("relevance", 0) for e in self.evaluation_history) / total_evals
        avg_coverage = sum(e.get("coverage", 0) for e in self.evaluation_history) / total_evals
        avg_diversity = sum(e.get("diversity", 0) for e in self.evaluation_history) / total_evals
        
        # Quality distribution
        quality_counts = {}
        for e in self.evaluation_history:
            q = e.get("overall_quality", "unknown")
            quality_counts[q] = quality_counts.get(q, 0) + 1
        
        return {
            "total_evaluations": total_evals,
            "average_relevance": avg_relevance,
            "average_coverage": avg_coverage,
            "average_diversity": avg_diversity,
            "quality_distribution": quality_counts,
            "recent_evaluations": self.evaluation_history[-10:]
        }
