"""
Reranker - Reranks retrieved results for relevance

Responsibilities:
- Scores results based on multiple relevance signals
- Reorders results from most to least relevant
- Filters out low-quality or off-topic results
- Ensures top results directly address user query

RANKING SIGNALS:
1. Semantic similarity to query
2. Engagement score (how much user interacted with item)
3. Recency (newer items ranked slightly higher if relevant)
4. Source type preference (some sources more reliable for different query types)
5. System group alignment (if query targets specific domain)
"""

import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta
import numpy as np

logger = logging.getLogger(__name__)


class Reranker:
    """Reranks search results for relevance."""
    
    def __init__(self, decay_days: int = 30, min_score_threshold: float = 0.3):
        """
        Initialize reranker.
        
        Args:
            decay_days: Days over which engagement score decays
            min_score_threshold: Minimum score to include result
        """
        self.decay_days = decay_days
        self.min_score_threshold = min_score_threshold
        
        # Source reliability weights
        self.source_weights = {
            "gmail": 1.2,      # Email is usually direct and relevant
            "chrome": 1.0,     # Web pages are baseline
            "youtube": 0.9,    # Videos slightly lower than web
        }
    
    def rerank(
        self,
        results: List[Dict[str, Any]],
        query_text: str,
        system_group_id: int = None,
        recency_boost: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Rerank results by relevance.
        
        Args:
            results: Results from blended search
            query_text: Original user query
            system_group_id: Target system group (if query-specific)
            recency_boost: Whether to boost recent items
            
        Returns:
            Reranked results with scores
        """
        
        if not results:
            logger.warning("No results to rerank")
            return []
        
        logger.info(f"Reranking {len(results)} results")
        
        # Score each result
        scored_results = []
        for result in results:
            score = self._calculate_score(
                result,
                query_text,
                system_group_id,
                recency_boost
            )
            
            if score >= self.min_score_threshold:
                result["rerank_score"] = score
                scored_results.append(result)
        
        # Sort by score descending
        ranked = sorted(scored_results, key=lambda x: x["rerank_score"], reverse=True)
        
        logger.info(f"Reranked to {len(ranked)} results (filtered {len(results) - len(ranked)} low-score items)")
        
        return ranked
    
    def _calculate_score(
        self,
        result: Dict[str, Any],
        query_text: str,
        system_group_id: int = None,
        recency_boost: bool = True
    ) -> float:
        """
        Calculate relevance score for a single result.
        
        Score components:
        - Semantic similarity (0-1): 40% weight
        - Engagement decay (0-1): 20% weight
        - Recency boost (0-0.15): 15% weight
        - Source reliability (0-0.2): 20% weight
        - System group match (0-0.15): 5% weight
        """
        
        # 1. Semantic similarity (from blend_score or similarity)
        semantic_score = min(result.get("similarity", result.get("blend_score", 0.5)) / 100, 1.0)
        semantic_weight = 0.40
        
        # 2. Engagement decay
        engagement_score = self._engagement_decay(result.get("engagement_score", 0))
        engagement_weight = 0.20
        
        # 3. Recency boost
        recency_score = 0.0
        if recency_boost:
            recency_score = self._recency_boost(result.get("created_at"))
        recency_weight = 0.15
        
        # 4. Source reliability
        source_type = result.get("source_type", "chrome")
        source_score = self.source_weights.get(source_type, 1.0) / 1.2  # Normalize
        source_weight = 0.20
        
        # 5. System group match
        group_score = 0.0
        if system_group_id and result.get("system_group_id") == system_group_id:
            group_score = 1.0
        group_weight = 0.05
        
        # Final score
        total_score = (
            semantic_score * semantic_weight +
            engagement_score * engagement_weight +
            recency_score * recency_weight +
            source_score * source_weight +
            group_score * group_weight
        )
        
        return total_score
    
    def _engagement_decay(self, engagement_score: float) -> float:
        """
        Normalize engagement score.
        Higher engagement = more relevant (user spent time on it).
        """
        # Assuming engagement_score is 0-100
        return min(engagement_score / 100.0, 1.0)
    
    def _recency_boost(self, created_at: str) -> float:
        """
        Boost score for recent items.
        
        Decays over decay_days:
        - Today: +0.15 boost
        - 7 days ago: +0.10 boost
        - 30 days ago: +0.0 boost
        - Older: -0.05 (penalty)
        """
        if not created_at:
            return 0.0
        
        try:
            # Parse created_at timestamp
            if isinstance(created_at, str):
                created = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            else:
                created = created_at
            
            now = datetime.now(created.tzinfo) if created.tzinfo else datetime.now()
            days_old = (now - created).days
            
            if days_old == 0:
                return 0.15  # Today - maximum boost
            elif days_old <= 7:
                return 0.10  # This week
            elif days_old <= self.decay_days:
                # Linear decay from 0.10 to 0.0
                return max(0.10 * (1 - days_old / self.decay_days), 0.0)
            else:
                return -0.05  # Older items get slight penalty
                
        except Exception as e:
            logger.error(f"Error calculating recency boost: {e}")
            return 0.0
    
    def filter_duplicates(
        self,
        results: List[Dict[str, Any]],
        duplicate_threshold: float = 0.95
    ) -> List[Dict[str, Any]]:
        """
        Remove near-duplicate results based on content similarity.
        
        Args:
            results: Ranked results
            duplicate_threshold: Similarity threshold (0-1) for duplicates
            
        Returns:
            Deduplicated results
        """
        if len(results) < 2:
            return results
        
        filtered = []
        for i, result in enumerate(results):
            is_duplicate = False
            
            # Check against already-added results
            for added in filtered:
                # Simple check: if same source + same domain/sender, likely duplicate
                if (result.get("source_type") == added.get("source_type") and
                    result.get("memory_id") != added.get("memory_id")):
                    
                    # Same email sender or chrome domain
                    if ((result.get("metadata", {}).get("sender_email") ==
                         added.get("metadata", {}).get("sender_email")) or
                        (result.get("metadata", {}).get("domain") ==
                         added.get("metadata", {}).get("domain"))):
                        
                        is_duplicate = True
                        break
            
            if not is_duplicate:
                filtered.append(result)
        
        logger.info(f"Filtered {len(results) - len(filtered)} duplicates")
        return filtered
    
    def apply_diversity(
        self,
        results: List[Dict[str, Any]],
        max_from_source: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Diversify results to avoid source bias.
        
        Ensures no single source dominates top results.
        
        Args:
            results: Ranked results
            max_from_source: Max results from any single source in top N
            
        Returns:
            Diversified result list
        """
        source_counts = {}
        diverse_results = []
        
        for result in results:
            source = result.get("source_type", "unknown")
            source_counts[source] = source_counts.get(source, 0) + 1
            
            if source_counts[source] <= max_from_source:
                diverse_results.append(result)
        
        return diverse_results


# ========================
# CONTEXTUAL RERANKING
# ========================

class ContextualReranker(Reranker):
    """
    Reranker that considers conversation context.
    Boosts results related to previous turns.
    """
    
    def rerank_with_context(
        self,
        results: List[Dict[str, Any]],
        query_text: str,
        conversation_history: List = None,
        system_group_id: int = None
    ) -> List[Dict[str, Any]]:
        """
        Rerank results while considering conversation context.
        
        Args:
            results: Results from search
            query_text: Current query
            conversation_history: Previous conversation turns
            system_group_id: Target system group
            
        Returns:
            Contextually reranked results
        """
        
        # Extract entities/topics from previous turns
        prev_topics = self._extract_topics_from_history(conversation_history or [])
        
        # Boost results mentioning previous topics
        for result in results:
            text = (result.get("title", "") + " " + result.get("text", "")).lower()
            
            for topic in prev_topics:
                if topic.lower() in text:
                    result["context_boost"] = 0.1
                    break
            else:
                result["context_boost"] = 0.0
        
        # Rerank with context boost
        ranked = self.rerank(results, query_text, system_group_id)
        
        # Apply context boost to scores
        for result in ranked:
            result["rerank_score"] += result.get("context_boost", 0)
        
        # Re-sort
        ranked = sorted(ranked, key=lambda x: x["rerank_score"], reverse=True)
        
        return ranked
    
    def _extract_topics_from_history(self, history: List) -> List[str]:
        """Extract key topics from conversation history."""
        topics = []
        for message in history[-3:]:  # Look at last 3 messages
            # Simple extraction: split by common keywords
            content = getattr(message, 'content', str(message)).lower()
            # TODO: Use NLP for better extraction
            topics.extend(content.split())
        
        return list(set(topics))  # Deduplicate
