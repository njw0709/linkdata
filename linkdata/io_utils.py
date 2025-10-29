"""
Flexible data I/O utilities for reading and writing various file formats.

This module provides format-agnostic data reading and writing functions that
automatically detect the file format from the file extension and use the
appropriate pandas I/O method.

Supported formats:
- CSV (.csv)
- Stata (.dta)
- Parquet (.parquet, .pq)
- Feather (.feather)
- Excel (.xlsx, .xls)
"""

from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, Optional, Union
import pandas as pd
import numpy as np


# Helper: sanitize DataFrame for CSV/Excel/Stata exports
def _sanitize_for_tabular(input_df: pd.DataFrame, mode: str = "string") -> pd.DataFrame:
    """
    Sanitize DataFrame for export to tabular formats.

    Parameters
    ----------
    input_df : pd.DataFrame
        Input DataFrame to sanitize
    mode : str, default "string"
        - "string": Convert all values to strings (for CSV/Excel)
        - "preserve": Keep numeric types, only fix problematic types (for Stata)
    """
    sanitized = input_df.copy()

    # Handle categoricals early to avoid downstream surprises
    for col_name in sanitized.columns:
        col = sanitized[col_name]
        if pd.api.types.is_categorical_dtype(col):
            sanitized[col_name] = col.astype("string").astype(object)

    # Datetime handling
    for col_name in sanitized.columns:
        col = sanitized[col_name]
        if pd.api.types.is_datetime64_any_dtype(col):
            series = col
            try:
                # If timezone-aware, drop tz info (local naive)
                if getattr(series.dt, "tz", None) is not None:
                    series = series.dt.tz_localize(None)
            except Exception:
                # Fallback: attempt to convert to datetime then drop tz
                series = pd.to_datetime(series, errors="coerce")
                series = series.dt.tz_localize(None)

            if mode == "preserve":
                # Keep as datetime for Stata (Stata supports datetime)
                sanitized[col_name] = series
            else:
                # Convert to ISO string for CSV/Excel
                sanitized[col_name] = series.dt.strftime("%Y-%m-%dT%H:%M:%S")

    # Timedelta handling
    for col_name in sanitized.columns:
        col = sanitized[col_name]
        if pd.api.types.is_timedelta64_dtype(col):
            total_seconds = col.dt.total_seconds()
            if mode == "preserve":
                # Keep as numeric for Stata
                as_int = total_seconds % 1 == 0
                sanitized[col_name] = np.where(
                    as_int, total_seconds.astype("Int64"), total_seconds.astype(float)
                )
            else:
                # Convert to string for CSV/Excel
                as_int = total_seconds % 1 == 0
                sanitized[col_name] = np.where(
                    as_int,
                    total_seconds.astype("Int64").astype(object),
                    total_seconds.astype(float),
                ).astype(str)

    # Booleans handling
    for col_name in sanitized.columns:
        col = sanitized[col_name]
        if pd.api.types.is_bool_dtype(col):
            if mode == "preserve":
                # Keep as int for Stata (0/1 numeric)
                sanitized[col_name] = col.astype(int)
            else:
                # Convert to string for CSV/Excel
                sanitized[col_name] = col.astype(int).astype(str)

    # Object columns: coerce element-wise
    def _coerce_object_value(value: Any, as_string: bool = True) -> Any:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        # Keep primitives that are already safe
        if isinstance(value, (str, int, float)):
            return value
        if isinstance(value, (np.integer, np.floating)):
            return value.item()
        if isinstance(value, (bool, np.bool_)):
            return (
                "1" if bool(value) else "0" if as_string else (1 if bool(value) else 0)
            )
        if isinstance(value, (bytes, bytearray)):
            try:
                return bytes(value).decode("utf-8", errors="replace")
            except Exception:
                return str(value)
        # Lists/dicts/numpy arrays -> repr()
        if isinstance(value, (list, dict, tuple, set, np.ndarray)):
            return repr(value)
        # Fallback to str()
        return str(value)

    for col_name in sanitized.columns:
        col = sanitized[col_name]
        if pd.api.types.is_object_dtype(col):
            as_string = mode != "preserve"
            sanitized[col_name] = col.map(
                lambda v: _coerce_object_value(v, as_string=as_string)
            )

    return sanitized


def read_data(
    file_path: Union[str, Path],
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Read data from a file, automatically detecting the format from the file extension.

    Supports CSV, Stata (.dta), Parquet, Feather, and Excel formats.

    Parameters
    ----------
    file_path : str or Path
        Path to the file to read.
    **kwargs : Any
        Additional keyword arguments to pass to the underlying pandas read function.
        Common examples:
        - usecols: List of columns to read
        - dtype: Dictionary of column dtypes
        - parse_dates: List of columns to parse as dates
        - chunksize: For CSV, return an iterator (not supported for other formats)

    Returns
    -------
    pd.DataFrame
        The loaded data as a pandas DataFrame.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    ValueError
        If the file extension is not supported.

    Examples
    --------
    >>> # Read a Stata file
    >>> df = read_data("data/survey.dta")

    >>> # Read a CSV with specific columns
    >>> df = read_data("data/measures.csv", usecols=["Date", "GEOID10", "Tmax"])

    >>> # Read a Parquet file
    >>> df = read_data("data/results.parquet")
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Get file extension (lowercase, without dot)
    ext = file_path.suffix.lower().lstrip(".")

    # Map extension to pandas read function
    if ext == "csv":
        return pd.read_csv(file_path, **kwargs)
    elif ext == "dta":
        return pd.read_stata(file_path, **kwargs)
    elif ext in ("parquet", "pq"):
        return pd.read_parquet(file_path, **kwargs)
    elif ext == "feather":
        return pd.read_feather(file_path, **kwargs)
    elif ext in ("xlsx", "xls"):
        return pd.read_excel(file_path, **kwargs)
    else:
        raise ValueError(
            f"Unsupported file format: '.{ext}'. "
            f"Supported formats: .csv, .dta, .parquet, .pq, .feather, .xlsx, .xls"
        )


def write_data(
    df: pd.DataFrame,
    file_path: Union[str, Path],
    **kwargs: Any,
) -> None:
    """
    Write DataFrame to a file, automatically detecting the format from the file extension.

    Supports CSV, Stata (.dta), Parquet, Feather, and Excel formats.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to write.
    file_path : str or Path
        Path to the output file.
    **kwargs : Any
        Additional keyword arguments to pass to the underlying pandas write function.
        Common examples:
        - index: Whether to write row index (default behavior varies by format)
        - compression: Compression type for supported formats

    Raises
    ------
    ValueError
        If the file extension is not supported.

    Notes
    -----
    - For Stata files, the index is not written by default (Stata doesn't support it).
    - For CSV files, index writing depends on the kwargs (default is True in pandas).
    - For Parquet and Feather, index writing depends on kwargs.

    Examples
    --------
    >>> # Write to Stata
    >>> write_data(df, "output/results.dta")

    >>> # Write to CSV without index
    >>> write_data(df, "output/results.csv", index=False)

    >>> # Write to Parquet with compression
    >>> write_data(df, "output/results.parquet", compression="gzip")
    """
    file_path = Path(file_path)

    # Create parent directory if it doesn't exist
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Get file extension (lowercase, without dot)
    ext = file_path.suffix.lower().lstrip(".")

    # Prepare a defensive copy for sanitation across all formats
    out_df = df.copy()

    # Map extension to pandas write function
    if ext == "csv":
        # Excel-first friendly CSV
        sanitized_df = _sanitize_for_tabular(out_df)
        write_kwargs = {"index": False, "encoding": "utf-8-sig", "na_rep": ""}
        write_kwargs.update(kwargs)
        sanitized_df.to_csv(file_path, **write_kwargs)
    elif ext == "dta":
        # Stata doesn't support index writing, so we don't pass it
        # Remove index parameter if it was passed
        write_kwargs = {k: v for k, v in kwargs.items() if k != "index"}

        # Apply sanitation for Stata with type preservation
        sanitized_df = _sanitize_for_tabular(out_df, mode="preserve")

        # Additional Stata-specific pass: ensure all object columns are truly string-or-None
        for name in sanitized_df.columns:
            col = sanitized_df[name]
            if pd.api.types.is_object_dtype(col):
                # Convert all values to str (or None for missing)
                sanitized_df[name] = col.astype(object).where(~pd.isna(col), None)

        sanitized_df.to_stata(file_path, **write_kwargs)
    elif ext in ("parquet", "pq"):
        out_df.to_parquet(file_path, **kwargs)
    elif ext == "feather":
        out_df.to_feather(file_path, **kwargs)
    elif ext in ("xlsx", "xls"):
        # Apply the same sanitation for Excel to ensure consistent, readable values
        sanitized_df = _sanitize_for_tabular(out_df)
        write_kwargs = {"index": False}
        write_kwargs.update(kwargs)
        sanitized_df.to_excel(file_path, **write_kwargs)
    else:
        raise ValueError(
            f"Unsupported file format: '.{ext}'. "
            f"Supported formats: .csv, .dta, .parquet, .pq, .feather, .xlsx, .xls"
        )


def get_file_format(file_path: Union[str, Path]) -> str:
    """
    Get the file format from a file path.

    Parameters
    ----------
    file_path : str or Path
        Path to the file.

    Returns
    -------
    str
        The file format (e.g., "csv", "dta", "parquet", "feather", "excel").

    Examples
    --------
    >>> get_file_format("data/file.csv")
    'csv'
    >>> get_file_format("data/file.parquet")
    'parquet'
    """
    file_path = Path(file_path)
    ext = file_path.suffix.lower().lstrip(".")

    if ext == "csv":
        return "csv"
    elif ext == "dta":
        return "stata"
    elif ext in ("parquet", "pq"):
        return "parquet"
    elif ext == "feather":
        return "feather"
    elif ext in ("xlsx", "xls"):
        return "excel"
    else:
        raise ValueError(
            f"Unsupported file format: '.{ext}'. "
            f"Supported formats: .csv, .dta, .parquet, .pq, .feather, .xlsx, .xls"
        )
