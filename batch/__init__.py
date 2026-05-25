"""
Batch processing module for CimaPerú.
"""

from .etl_senamhi import SenamhiETL, main

__all__ = ["SenamhiETL", "main"]