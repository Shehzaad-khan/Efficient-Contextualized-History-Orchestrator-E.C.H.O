"""
Search Coordinator - Blends SQL + FAISS Vector Search

Responsibilities:
- Coordinates SQL keyword search with FAISS semantic search
- Implements fallback behavior when search returns insufficient results
- Filters and blends results from both search methods
- Ensures deterministic retrieval behavior

RETRIEVAL BLEND STRATEGY:
1. Primary: SQL search using parsed_intent filters (domain, time, source)
2. Secondary: FAISS vector search on candidate set
3. Fallback: Expand search scope if results are weak
4. Final: Rank and deduplicate combined results
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
import psycopg2
import numpy as np
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class SearchCoordinator:
    """Coordinates SQL and vector search for retrieval."""
    
    def __init__(self, db_url: str, faiss_manager, min_results_threshold: int = 5):
        """
        Initialize search coordinator.
        
        Args:
            db_url: PostgreSQL connection URL
            faiss_manager: FAISS manager instance for vector search
            min_results_threshold: Minimum results needed; triggers fallback if below this
        """
        self.db_url = db_url
        self.faiss_manager = faiss_manager
        self.min_results_threshold = min_results_threshold
        
    def _get_db_connection(self):
        """Get PostgreSQL connection."""
        return psycopg2.connect(self.db_url)
    
    # ========================
    # SQL SEARCH
    # ========================
    
    def sql_search(
        self,
        query_text: str,
        parsed_intent: Dict[str, Any],
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Execute SQL search using parsed intent filters.
        
        Args:
            query_text: User's search query
            parsed_intent: Parsed query intent with filters
            limit: Max results to return
            
        Returns:
            List of candidate items from database
        """
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Build dynamic SQL query
            sql = """
                SELECT 
                    mi.memory_id,
                    mi.title,
                    mi.source_type,
                    mi.clean_text,
                    mi.system_group_id,
                    mi.created_at,
                    mi.engagement_score,
                    gm.sender_email,
                    cm.domain,
                    ym.video_title,
                    ym.channel_name
                FROM memory_items mi
                LEFT JOIN gmail_metadata gm ON mi.memory_id = gm.memory_id
                LEFT JOIN chrome_metadata cm ON mi.memory_id = cm.memory_id
                LEFT JOIN youtube_metadata ym ON mi.memory_id = ym.memory_id
                WHERE 1=1
            """
            
            params = []
            
            # Apply source filter
            sources = parsed_intent.get("sources", ["all"])
            if sources and sources != ["all"]:
                source_list = "({})".format(",".join(["%s"] * len(sources)))
                sql += f" AND mi.source_type IN {source_list}"
                params.extend(sources)
            
            # Apply time filter
            time_filter = parsed_intent.get("time_filter")
            if time_filter:
                if time_filter == "today":
                    sql += " AND mi.created_at >= CURRENT_DATE"
                elif time_filter == "week":
                    sql += " AND mi.created_at >= CURRENT_DATE - INTERVAL '7 days'"
                elif time_filter == "month":
                    sql += " AND mi.created_at >= CURRENT_DATE - INTERVAL '30 days'"
            
            # Apply keyword search
            keywords = parsed_intent.get("keywords", [])
            if keywords:
                keyword_pattern = " OR ".join(["%s"] * len(keywords))
                sql += f" AND (mi.title ILIKE %s OR mi.clean_text ILIKE %s)"
                for keyword in keywords:
                    params.extend([f"%{keyword}%", f"%{keyword}%"])
            
            # Apply system group filter (if specified)
            system_group = parsed_intent.get("system_group")
            if system_group:
                sql += " AND mi.system_group_id = %s"
                params.append(system_group)
            
            sql += f" ORDER BY mi.engagement_score DESC, mi.created_at DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            
            # Convert to dict format
            results = [
                {
                    "memory_id": row[0],
                    "title": row[1],
                    "source_type": row[2],
                    "text": row[3],
                    "system_group_id": row[4],
                    "created_at": row[5],
                    "engagement_score": row[6],
                    "metadata": {
                        "sender_email": row[7],
                        "domain": row[8],
                        "video_title": row[9],
                        "channel_name": row[10]
                    },
                    "search_method": "sql"
                }
                for row in rows
            ]
            
            logger.info(f"SQL search returned {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"SQL search failed: {e}")
            return []
        finally:
            cursor.close()
            conn.close()
    
    # ========================
    # FAISS VECTOR SEARCH
    # ========================
    
    def vector_search(
        self,
        query_embedding: np.ndarray,
        candidate_ids: List[str],
        k: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Execute FAISS vector search on candidate set.
        
        Args:
            query_embedding: Query embedding vector (384-dim)
            candidate_ids: Memory IDs to search within
            k: Top-k results to return
            
        Returns:
            List of items sorted by similarity
        """
        try:
            # Search FAISS index
            results = self.faiss_manager.search(
                query_embedding,
                candidate_ids,
                k=k
            )
            
            logger.info(f"Vector search returned {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            return []
    
    # ========================
    # BLENDED SEARCH
    # ========================
    
    def blended_search(
        self,
        query_text: str,
        query_embedding: np.ndarray,
        parsed_intent: Dict[str, Any]
    ) -> Tuple[List[Dict[str, Any]], str]:
        """
        Execute blended SQL + vector search with fallback.
        
        Strategy:
        1. Run SQL search for keyword + filter match
        2. Run vector search on SQL candidate set
        3. Blend and deduplicate results
        4. If results weak, expand scope (fallback)
        
        Args:
            query_text: User query
            query_embedding: Query embedding vector
            parsed_intent: Parsed query intent
            
        Returns:
            Tuple of (blended_results, quality_assessment)
        """
        
        # Step 1: SQL Search
        sql_results = self.sql_search(query_text, parsed_intent, limit=100)
        
        if not sql_results:
            logger.warning("SQL search returned no results - triggering fallback")
            return self._fallback_search(query_embedding, parsed_intent), "empty"
        
        # Step 2: Vector Search on SQL candidates
        candidate_ids = [r["memory_id"] for r in sql_results]
        vector_results = self.vector_search(query_embedding, candidate_ids, k=20)
        
        # Step 3: Blend results
        blended = self._blend_results(sql_results, vector_results)
        
        # Step 4: Assess quality
        quality = self._assess_result_quality(blended)
        
        # Step 5: Fallback if weak
        if quality == "weak" and len(blended) < self.min_results_threshold:
            logger.warning("Results weak - expanding search scope")
            blended = self._fallback_search(query_embedding, parsed_intent)
            quality = "fallback"
        
        return blended, quality
    
    def _blend_results(
        self,
        sql_results: List[Dict],
        vector_results: List[Dict]
    ) -> List[Dict]:
        """
        Blend SQL and vector results, deduplicating by memory_id.
        
        Scoring:
        - SQL exact match: score +100
        - Vector similarity: score += similarity * 100
        - Final: sort by combined score
        """
        seen = {}
        
        # Add SQL results (keyword relevance)
        for result in sql_results:
            memory_id = result["memory_id"]
            result["blend_score"] = 100.0  # Exact match bonus
            seen[memory_id] = result
        
        # Add/update with vector results (semantic relevance)
        for result in vector_results:
            memory_id = result["memory_id"]
            similarity = result.get("similarity", 0.0)
            
            if memory_id in seen:
                # Already in SQL results - add vector bonus
                seen[memory_id]["blend_score"] += (similarity * 50)
            else:
                # New from vector search
                result["blend_score"] = similarity * 100
                result["search_method"] = "vector"
                seen[memory_id] = result
        
        # Sort by blend score
        blended = sorted(seen.values(), key=lambda x: x["blend_score"], reverse=True)
        
        logger.info(f"Blended {len(sql_results)} SQL + {len(vector_results)} vector results = {len(blended)} unique items")
        return blended
    
    def _assess_result_quality(self, results: List[Dict]) -> str:
        """
        Assess quality of results.
        
        Returns:
            "strong" if high relevance + sufficient results
            "weak" if low relevance or few results
            "empty" if no results
        """
        if not results:
            return "empty"
        
        if len(results) < self.min_results_threshold:
            return "weak"
        
        # Check average blend score
        avg_score = sum(r.get("blend_score", 0) for r in results) / len(results)
        
        if avg_score > 80:
            return "strong"
        elif avg_score > 40:
            return "weak"
        else:
            return "weak"
    
    def _fallback_search(
        self,
        query_embedding: np.ndarray,
        parsed_intent: Dict[str, Any]
    ) -> List[Dict]:
        """
        Fallback search when primary retrieval is weak.
        
        Strategy:
        - Remove time filter (search all history)
        - Remove source filter (search all sources)
        - Do broad vector search
        """
        logger.warning("Triggering fallback: expanding search scope")
        
        # Expand parsed_intent
        expanded_intent = parsed_intent.copy()
        expanded_intent["sources"] = ["all"]
        expanded_intent["time_filter"] = None
        
        # Run broad SQL search
        sql_results = self.sql_search("", expanded_intent, limit=200)
        
        if not sql_results:
            logger.warning("Fallback SQL search also failed")
            return []
        
        # Run vector search on expanded candidate set
        candidate_ids = [r["memory_id"] for r in sql_results]
        vector_results = self.vector_search(query_embedding, candidate_ids, k=30)
        
        # Blend expanded results
        blended = self._blend_results(sql_results, vector_results)
        
        return blended[:50]  # Return top 50 fallback results
