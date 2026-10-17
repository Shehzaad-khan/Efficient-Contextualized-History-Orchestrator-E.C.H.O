"""
faiss_manager.py — compatibility alias for ste.faiss_manager (ENP module)
Echo Personal Memory System

The FAISS index code lives in ste/faiss_manager.py (STE owns storage). This
file only keeps an old import path working.

One caller is left: scripts/enp_recompute_centroids.py (`get_manager`). Every
other module imports ste.faiss_manager directly. Planned removal (tracker C4,
step 7): point that script at ste.faiss_manager, then delete this file.
"""

from __future__ import annotations

from ste.faiss_manager import get_manager

__all__ = ["get_manager"]
