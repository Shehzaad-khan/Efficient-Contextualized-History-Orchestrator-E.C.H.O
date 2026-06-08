"""
Synthesizer - Synthesizes final answer from retrieved results

Responsibilities:
- Combines top-ranked results into coherent answer
- Cites sources for all claims
- Handles multi-turn context
- Adds relevant metadata (dates, creators, etc.)

SYNTHESIS STRATEGY:
1. Summarize top 3-5 most relevant results
2. For each result, include citation (source + date)
3. Synthesize into cohesive narrative
4. Add follow-up suggestions
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class Synthesizer:
    """Synthesizes answers from retrieved results."""
    
    def __init__(self, llm_client=None, use_llm: bool = True):
        """
        Initialize synthesizer.
        
        Args:
            llm_client: LLM client for synthesis (Claude, Gemini, etc.)
            use_llm: Whether to use LLM or template-based synthesis
        """
        self.llm_client = llm_client
        self.use_llm = use_llm
    
    def synthesize(
        self,
        query: str,
        results: List[Dict[str, Any]],
        conversation_history: List = None,
        source_type_filter: str = None
    ) -> Dict[str, Any]:
        """
        Synthesize final answer from results.
        
        Args:
            query: User's original query
            results: Top-ranked results from retrieval
            conversation_history: Previous conversation context
            source_type_filter: If multi-source, filter to specific source
            
        Returns:
            Dict with synthesized answer, citations, and metadata
        """
        
        if not results:
            return {
                "answer": "I couldn't find any relevant information for your query. Please try rephrasing or expanding your search.",
                "citations": [],
                "result_count": 0,
                "synthesis_method": "no_results"
            }
        
        logger.info(f"Synthesizing answer from {len(results)} results")
        
        # Take top results (limit to 5)
        top_results = results[:5]
        
        # Choose synthesis method
        if self.use_llm and self.llm_client:
            answer_data = self._llm_synthesize(query, top_results, conversation_history)
        else:
            answer_data = self._template_synthesize(query, top_results)
        
        # Extract citations
        citations = self._extract_citations(top_results)
        
        # Add metadata
        answer_data["citations"] = citations
        answer_data["result_count"] = len(results)
        answer_data["top_results_used"] = len(top_results)
        
        return answer_data
    
    def _llm_synthesize(
        self,
        query: str,
        results: List[Dict[str, Any]],
        conversation_history: List = None
    ) -> Dict[str, Any]:
        """
        Use LLM to synthesize answer.
        """
        
        try:
            # Build context from results
            context_text = self._build_context(results)
            
            # Build prompt
            system_prompt = """You are an AI assistant helping users find information from their personal digital history.
Your task is to synthesize a concise, helpful answer based on the provided context.

RULES:
1. Be concise (2-3 sentences for simple queries, 5-7 for complex ones)
2. Only use information from the provided context
3. If context is insufficient, say so clearly
4. Maintain conversational tone
5. Reference the source type (email, webpage, video) naturally in your answer"""
            
            user_prompt = f"""User Query: {query}

Context from their digital history:
{context_text}

Provide a concise, helpful answer based only on the above context. If the context is insufficient to answer, explain what additional information would help."""
            
            # Call LLM
            response = self.llm_client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=500,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}]
            )
            
            answer_text = response.content[0].text
            
            logger.info("LLM synthesis completed")
            
            return {
                "answer": answer_text,
                "synthesis_method": "llm"
            }
            
        except Exception as e:
            logger.error(f"LLM synthesis failed: {e}, falling back to template")
            return self._template_synthesize(query, results)
    
    def _template_synthesize(
        self,
        query: str,
        results: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Template-based synthesis without LLM.
        Useful for deterministic, fast answers.
        """
        
        if len(results) == 0:
            return {
                "answer": "No results found.",
                "synthesis_method": "template"
            }
        
        # Build answer from top result
        top = results[0]
        source = top.get("source_type", "unknown")
        title = top.get("title", "Item")
        created_at = top.get("created_at", "")
        
        # Format date
        date_str = ""
        if created_at:
            try:
                date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                date_str = f" from {date.strftime('%B %d, %Y')}"
            except:
                pass
        
        # Build answer
        if source == "gmail":
            sender = top.get("metadata", {}).get("sender_email", "an email")
            answer = f"I found an email{date_str} about '{title}' from {sender}. It contains relevant information about your query."
        
        elif source == "chrome":
            domain = top.get("metadata", {}).get("domain", "a website")
            answer = f"I found a webpage{date_str} from {domain} titled '{title}' that discusses this topic."
        
        elif source == "youtube":
            channel = top.get("metadata", {}).get("channel_name", "a YouTube channel")
            answer = f"I found a video{date_str} on the channel '{channel}' about '{title}' related to your query."
        
        else:
            answer = f"I found '{title}'{date_str} that's relevant to your question."
        
        # Add follow-up if multiple results
        if len(results) > 1:
            answer += f" I also found {len(results) - 1} other relevant items."
        
        return {
            "answer": answer,
            "synthesis_method": "template"
        }
    
    def _build_context(self, results: List[Dict[str, Any]]) -> str:
        """
        Build context string from results for LLM input.
        """
        context_parts = []
        
        for i, result in enumerate(results[:5], 1):
            source = result.get("source_type", "unknown")
            title = result.get("title", "Untitled")
            text = result.get("text", result.get("clean_text", ""))[:300]  # First 300 chars
            
            # Truncate text for LLM token limit
            if len(text) > 300:
                text = text[:297] + "..."
            
            source_info = {
                "gmail": f"Email from {result.get('metadata', {}).get('sender_email', 'unknown')}",
                "chrome": f"Webpage from {result.get('metadata', {}).get('domain', 'unknown')}",
                "youtube": f"Video from {result.get('metadata', {}).get('channel_name', 'YouTube')}"
            }.get(source, f"{source} item")
            
            part = f"{i}. [{source_info}] {title}\n   {text}"
            context_parts.append(part)
        
        return "\n\n".join(context_parts)
    
    def _extract_citations(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract citations for all results.
        """
        citations = []
        
        for result in results[:5]:  # Top 5
            source = result.get("source_type", "unknown")
            title = result.get("title", "Untitled")
            created_at = result.get("created_at", "")
            
            # Format date
            date_str = "Unknown date"
            if created_at:
                try:
                    date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    date_str = date.strftime('%B %d, %Y')
                except:
                    pass
            
            # Build citation
            if source == "gmail":
                citation = {
                    "type": "Email",
                    "title": title,
                    "from": result.get("metadata", {}).get("sender_email", "Unknown"),
                    "date": date_str,
                    "url": None  # Emails don't have URLs
                }
            
            elif source == "chrome":
                citation = {
                    "type": "Webpage",
                    "title": title,
                    "domain": result.get("metadata", {}).get("domain", "Unknown"),
                    "date": date_str,
                    "url": None  # URL not stored in this schema
                }
            
            elif source == "youtube":
                citation = {
                    "type": "YouTube Video",
                    "title": title,
                    "channel": result.get("metadata", {}).get("channel_name", "Unknown"),
                    "date": date_str,
                    "url": None  # Video URL not stored
                }
            
            else:
                citation = {
                    "type": source.capitalize(),
                    "title": title,
                    "date": date_str
                }
            
            citation["relevance_score"] = result.get("rerank_score", 0)
            citations.append(citation)
        
        return citations
    
    def format_answer(
        self,
        synthesis_result: Dict[str, Any],
        include_citations: bool = True
    ) -> str:
        """
        Format final answer for display to user.
        
        Args:
            synthesis_result: Output from synthesize()
            include_citations: Whether to include citation section
            
        Returns:
            Formatted answer string
        """
        
        answer = synthesis_result.get("answer", "")
        
        if not include_citations:
            return answer
        
        # Add citations section
        citations = synthesis_result.get("citations", [])
        if citations:
            answer += "\n\n**Sources:**\n"
            for i, citation in enumerate(citations, 1):
                citation_str = f"{i}. {citation['type']}: {citation['title']} ({citation['date']})"
                if citation.get('from'):
                    citation_str += f" from {citation['from']}"
                elif citation.get('channel'):
                    citation_str += f" from {citation['channel']}"
                elif citation.get('domain'):
                    citation_str += f" - {citation['domain']}"
                
                answer += f"\n{citation_str}"
        
        return answer
