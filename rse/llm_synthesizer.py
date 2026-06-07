"""
synthesize node — LLM Call 2 (stub for this phase).

Real implementation (Phase 3): assembles context from top-10 re-ranked results,
attachment content, and conversation history. Generates a readable answer with
source citations, temporal context, and engagement depth metadata.

The provider factory here mirrors query_parser._build_provider_llm but uses
LLM_CONFIG['synthesizer_model'] so parser and synthesizer can use different
model tiers.
"""
import logging
from typing import Any

from rse.config import LLM_CONFIG
from rse.state import EchoState

logger = logging.getLogger(__name__)


def build_synthesizer_llm() -> Any:
    """
    Instantiate the synthesis LLM via LangChain's universal init_chat_model factory.

    Reads provider + synthesizer_model from LLM_CONFIG. API keys are read from
    environment variables by the integration package. Same provider as the parser
    unless you set a different model tier in LLM_CONFIG['synthesizer_model'].

    Returns:
        A LangChain chat model instance.
    """
    from langchain.chat_models import init_chat_model
    return init_chat_model(
        model=LLM_CONFIG["synthesizer_model"],
        model_provider=LLM_CONFIG["provider"],
        temperature=LLM_CONFIG.get("synthesizer_temperature", 0.3),
    )
