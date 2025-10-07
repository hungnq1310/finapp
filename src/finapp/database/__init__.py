"""
Database Package for Financial News Analysis

This package provides database implementations and abstractions.
"""

from .abstract import DataRepository
from .mongo import MongoDataRepository

__all__ = [
    "DataRepository",
    "MongoDataRepository",
]
