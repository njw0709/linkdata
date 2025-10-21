"""
Linkdata package for linking survey data with contextual daily measures.
"""

try:
    from importlib.metadata import version, PackageNotFoundError

    try:
        __version__ = version("linkage")
    except PackageNotFoundError:
        __version__ = "0.1.0"  # Fallback version
except ImportError:
    __version__ = "0.1.0"  # Fallback for Python < 3.8

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
    # Version
    "__version__",
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
