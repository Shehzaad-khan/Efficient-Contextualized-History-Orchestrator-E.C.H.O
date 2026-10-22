"""
One place that builds every LLM client in Echo (decision C21, 2026-10-18).

Rule (decision A25): plug-in for every provider, specialised for OpenAI.
All clients come from LangChain's init_chat_model, so switching provider means
changing only rse.config.LLM_CONFIG. When the provider is "openai" and
LLM_CONFIG sets reasoning_effort, the OpenAI extras are switched on —
reasoning effort and (if use_responses_api) the Responses API — and no
temperature is sent (reasoning models do not take one). Any other provider,
or OpenAI with reasoning_effort unset, gets the configured temperature.
Provider-specific extras for other vendors can be added here later the same way.

Callers: rse/query_parser.py and enp/system_group_classifier.py (Stage 4)
(parser_model); rse/llm_synthesizer.py and wba/insight_generator.py
(synthesizer_model).

Every model gets LLM_CONFIG["max_retries"] and ["timeout"] (C13).

Each (model, temperature) pair is built once and cached: the client keeps its
HTTP connection warm, and LangChain chat models are safe to share between
request threads.
"""
import logging
from functools import lru_cache
from typing import Any

from rse.config import LLM_CONFIG

logger = logging.getLogger(__name__)


@lru_cache(maxsize=None)
def build_chat_model(model_key: str, temperature_key: str) -> Any:
    """
    Build (once) the chat model named by LLM_CONFIG[model_key].

    Args:
        model_key: LLM_CONFIG key holding the model name, e.g. "parser_model".
        temperature_key: LLM_CONFIG key holding the temperature used when the
            reasoning path is off, e.g. "parser_temperature".

    Returns:
        A LangChain chat model instance.

    Raises:
        KeyError: model_key is not in LLM_CONFIG.
    """
    from langchain.chat_models import init_chat_model

    provider = LLM_CONFIG["provider"]
    kwargs: dict[str, Any] = {
        "model": LLM_CONFIG[model_key],
        "model_provider": provider,
        "max_retries": LLM_CONFIG.get("max_retries", 2),
        "timeout": LLM_CONFIG.get("timeout"),
    }
    effort = LLM_CONFIG.get("reasoning_effort")
    if provider == "openai" and effort:
        if LLM_CONFIG.get("use_responses_api", False):
            # Responses API takes a reasoning object.
            kwargs["use_responses_api"] = True
            kwargs["reasoning"] = {"effort": effort}
        else:
            # Chat Completions takes a flat reasoning_effort string.
            kwargs["use_responses_api"] = False
            kwargs["reasoning_effort"] = effort
    else:
        temperature = LLM_CONFIG.get(temperature_key)
        if temperature is not None:
            kwargs["temperature"] = temperature
    logger.info("LLM client ready for %s: %s / %s (effort=%s, responses_api=%s, temperature=%s)",
                model_key, provider, kwargs["model"], effort if provider == "openai" else None,
                kwargs.get("use_responses_api", False), kwargs.get("temperature"))
    return init_chat_model(**kwargs)
