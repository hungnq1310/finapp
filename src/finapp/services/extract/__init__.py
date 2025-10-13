"""
Extract Service Package

This package provides content extraction functionality for crawled articles.
"""

from .html_content import HTMLContentExtractor, create_html_extractor

__all__ = [
    "HTMLContentExtractor",
    "create_html_extractor",
]