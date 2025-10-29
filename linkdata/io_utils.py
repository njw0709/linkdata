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
        out_df.to_csv(file_path, **kwargs)
    elif ext == "dta":
        # Stata doesn't support index writing, so we don't pass it
        # Remove index parameter if it was passed
        write_kwargs = {k: v for k, v in kwargs.items() if k != "index"}

        # 2) Stata-specific sanitation: ensure object/string cols are string-or-None only
        #    and convert categoricals to object strings
        for name in out_df.columns:
            col = out_df[name]
            if pd.api.types.is_categorical_dtype(col):
                out_df[name] = col.astype("string").astype(object)
            elif pd.api.types.is_string_dtype(col):
                # Convert pandas StringDtype to Python objects (str or None)
                out_df[name] = col.astype(object).where(~col.isna(), None)
            elif pd.api.types.is_object_dtype(col):
                # Replace pandas NA with None and ensure values are string-like or None
                series_obj = col.where(~pd.isna(col), None)
                out_df[name] = series_obj

        out_df.to_stata(file_path, **write_kwargs)
    elif ext in ("parquet", "pq"):
        out_df.to_parquet(file_path, **kwargs)
    elif ext == "feather":
        out_df.to_feather(file_path, **kwargs)
    elif ext in ("xlsx", "xls"):
        out_df.to_excel(file_path, **kwargs)
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
