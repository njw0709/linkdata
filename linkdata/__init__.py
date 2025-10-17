"""
Linkdata package for linking survey data with contextual daily measures.
"""

from .hrs import ResidentialHistoryHRS, HRSInterviewData, HRSContextLinker
from .daily_measure import DailyMeasureData, DailyMeasureDataDir
from .io_utils import read_data, write_data, get_file_format
from .process import (
    compute_required_years,
    extract_unique_geoids,
    process_multiple_lags_batch,
    process_multiple_lags_parallel,
)

__all__ = [
    # HRS classes
    "ResidentialHistoryHRS",
    "HRSInterviewData",
    "HRSContextLinker",
    # Daily measure classes
    "DailyMeasureData",
    "DailyMeasureDataDir",
    # I/O utilities
    "read_data",
    "write_data",
    "get_file_format",
    # Processing functions
    "compute_required_years",
    "extract_unique_geoids",
    "process_multiple_lags_batch",
    "process_multiple_lags_parallel",
]
