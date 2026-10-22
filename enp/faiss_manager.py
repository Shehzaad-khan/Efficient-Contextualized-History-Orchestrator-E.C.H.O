"""
faiss_manager.py — compatibility alias for ste.faiss_manager (ENP module)
Echo Personal Memory System

The FAISS index code lives in ste/faiss_manager.py (STE owns storage). This
file only keeps an old import path working.

No caller is left: its last user, scripts/enp_recompute_centroids.py, was
deleted on 2026-10-19, and every module imports ste.faiss_manager directly.
Planned removal (tracker C4, step 7): delete this file.
"""

from __future__ import annotations

from ste.faiss_manager import get_manager

__all__ = ["get_manager"]
