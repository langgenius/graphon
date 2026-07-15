"""Orchestration subsystem for graph engine.

This package coordinates the overall execution flow between
different subsystems.
"""

from .dispatcher import Dispatcher

__all__ = ["Dispatcher"]
