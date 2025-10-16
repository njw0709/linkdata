from pathlib import Path
from typing import Optional, List
import pandas as pd
from .hrs import HRSInterviewData, HRSContextLinker
from .daily_measure import DailyMeasureDataDir


def compute_required_years(
    hrs_data: HRSInterviewData,
    max_lag_days: int,
    date_col: Optional[str] = None,
) -> List[int]:
    """
    Compute which years of contextual data are needed based on:
    - Interview dates in HRS data
    - Maximum lag period

    This helps optimize data loading by only loading years that are actually
    needed for the linkage, avoiding loading unnecessary years.

    Parameters
    ----------
    hrs_data : HRSInterviewData
        HRS interview or epigenetic data object containing date information.
    max_lag_days : int
        Maximum lag period in days to consider. For example, if processing
        lags from 0 to 180 days, pass 180.
    date_col : str, optional
        Name of the date column to use. If None, uses hrs_data.datecol.

    Returns
    -------
    List[int]
        List of years needed for data linkage, sorted in ascending order.

    Examples
    --------
    >>> # Survey data from 2016-2020, processing up to 180-day lags
    >>> required_years = compute_required_years(hrs_data, max_lag_days=180)
    >>> # Returns [2015, 2016, 2017, 2018, 2019, 2020]
    >>> # (includes 2015 for 180-day lags from early 2016 dates)
    """
    if date_col is None:
        date_col = hrs_data.datecol

    dates = hrs_data.df[date_col]
    min_date = dates.min() - pd.Timedelta(days=max_lag_days)
    max_date = dates.max()

    return list(range(min_date.year, max_date.year + 1))


def extract_unique_geoids(
    hrs_data_with_lags: pd.DataFrame,
    geoid_prefix: str = "LINKCEN",
) -> set:
    """
    Extract all unique GEOIDs from n-day-prior GEOID columns in the DataFrame.

    This function identifies all GEOID columns (those containing the geoid_prefix)
    and collects unique values across all of them. This is useful for filtering
    contextual data to only include GEOIDs that are actually needed.

    Parameters
    ----------
    hrs_data_with_lags : pd.DataFrame
        DataFrame containing GEOID columns (typically output from
        HRSContextLinker.prepare_lag_columns_batch).
    geoid_prefix : str, default "LINKCEN"
        Prefix used to identify GEOID columns.

    Returns
    -------
    set
        Set of unique GEOID strings needed for contextual data loading.

    Examples
    --------
    >>> hrs_with_lags = HRSContextLinker.prepare_lag_columns_batch(
    ...     hrs_data, n_days=[0, 7, 30]
    ... )
    >>> unique_geoids = extract_unique_geoids(hrs_with_lags)
    >>> print(f"Need data for {len(unique_geoids)} unique GEOIDs")
    """
    geoid_cols = [c for c in hrs_data_with_lags.columns if geoid_prefix in c]
    all_geoids = set()

    for col in geoid_cols:
        geoids = hrs_data_with_lags[col].dropna().unique()
        all_geoids.update(geoids)

    return all_geoids


def process_multiple_lags_batch(
    hrs_data: HRSInterviewData,
    contextual_dir: DailyMeasureDataDir,
    n_days: List[int],
    id_col: str,
    temp_dir: Path,
    prefix: str = "",
    geoid_prefix: str = "LINKCEN",
    include_lag_date: bool = False,
    file_format: str = "parquet",
) -> List[Path]:
    """
    Process multiple lags with batch optimization using pre-computed columns and filtering.

    Workflow:
    1. Pre-compute all date/GEOID columns for all lags
    2. Keep in memory (faster than temp files for typical dataset sizes)
    3. Extract unique GEOIDs from all lag columns
    4. Load filtered contextual data once
    5. For each lag, merge pre-computed columns with contextual data
    6. Save each lag result to temp file

    Parameters
    ----------
    hrs_data : HRSInterviewData
        HRS interview or epigenetic data object
    contextual_dir : DailyMeasureDataDir
        Directory containing contextual daily measure data
    n_days : List[int]
        List of lag periods (in days) to process
    id_col : str
        Unique identifier column for joining (e.g., "hhidpn")
    temp_dir : Path
        Directory to save temporary lag files
    prefix : str, optional
        Prefix for output filenames
    geoid_prefix : str, default "LINKCEN"
        Prefix for GEOID column names
    include_lag_date : bool, default False
        Whether to include lag date columns in output
    file_format : {"parquet", "feather", "csv"}, default "parquet"
        File format for temporary output files

    Returns
    -------
    List[Path]
        List of paths to temporary files created for each lag
    """
    from .hrs import HRSContextLinker

    print(f"\n🔄 Starting batch processing for {len(n_days)} lags...")

    # Step 1: Pre-compute all lag columns
    print(f"📋 Pre-computing date/GEOID columns for lags: {n_days}")
    hrs_with_lags = HRSContextLinker.prepare_lag_columns_batch(
        hrs_data, n_days, geoid_prefix
    )

    # Step 2: Extract unique GEOIDs
    unique_geoids = extract_unique_geoids(hrs_with_lags, geoid_prefix)
    print(f"🔍 Extracted {len(unique_geoids)} unique GEOIDs from all lag columns")

    # Step 3: Compute required years and load filtered contextual data
    max_lag = max(n_days)
    required_years = compute_required_years(hrs_data, max_lag)
    available_years = set(contextual_dir.list_years())
    years_to_load = [str(y) for y in required_years if str(y) in available_years]
    print(f"📅 Loading years: {years_to_load}")

    # Set filter and preload
    contextual_dir.geoid_filter = unique_geoids
    contextual_dir.preload_years(years_to_load)

    # Concatenate all years
    print(f"🔗 Concatenating filtered contextual data...")
    contextual_df = pd.concat([contextual_dir[yr].df for yr in years_to_load], axis=0)
    print(f"  Contextual data shape: {contextual_df.shape}")

    # Step 4: Process each lag using pre-computed data
    temp_files = []
    for n in n_days:
        print(f"  Processing lag {n}...")

        out_df = HRSContextLinker.output_merged_columns(
            hrs_data,
            contextual_dir,
            n=n,
            id_col=id_col,
            precomputed_lag_df=hrs_with_lags,
            preloaded_contextual_df=contextual_df,
            include_lag_date=include_lag_date,
            geoid_prefix=geoid_prefix,
        )

        # Skip if no valid data
        if out_df.shape[1] <= 1:
            continue

        # Save to temp file
        filename = f"{prefix}_lag_{n:04d}.{file_format}"
        temp_file = temp_dir / filename

        if file_format == "parquet":
            out_df.to_parquet(temp_file, index=False)
        elif file_format == "feather":
            out_df.reset_index(drop=True).to_feather(temp_file)
        elif file_format == "csv":
            out_df.to_csv(temp_file, index=False)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        temp_files.append(temp_file)
        print(f"    ✓ Saved to {temp_file.name}")

    print(f"✅ Batch processing complete! Generated {len(temp_files)} files\n")
    return temp_files


def process_multiple_lags_parallel(
    hrs_data: HRSInterviewData,
    contextual_dir: DailyMeasureDataDir,
    n_days: List[int],
    id_col: str,
    temp_dir: Path,
    prefix: str = "",
    geoid_prefix: str = "LINKCEN",
    include_lag_date: bool = False,
    file_format: str = "parquet",
    max_workers: Optional[int] = None,
) -> List[Path]:
    """
    Process multiple lags with parallel processing using ThreadPoolExecutor.

    Pre-computes all lag columns and filters contextual data once, then processes
    lags in parallel threads that share the same memory space (avoiding serialization).

    Parameters
    ----------
    hrs_data : HRSInterviewData
        HRS interview or epigenetic data object
    contextual_dir : DailyMeasureDataDir
        Directory containing contextual daily measure data
    n_days : List[int]
        List of lag periods (in days) to process
    id_col : str
        Unique identifier column for joining (e.g., "hhidpn")
    temp_dir : Path
        Directory to save temporary lag files
    prefix : str, optional
        Prefix for output filenames
    geoid_prefix : str, default "LINKCEN"
        Prefix for GEOID column names
    include_lag_date : bool, default False
        Whether to include lag date columns in output
    file_format : {"parquet", "feather", "csv"}, default "parquet"
        File format for temporary output files
    max_workers : int, optional
        Maximum number of worker threads. If None, uses default from ThreadPoolExecutor.

    Returns
    -------
    List[Path]
        List of paths to temporary files created for each lag
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from .hrs import HRSContextLinker

    print(f"\n🚀 Starting parallel processing for {len(n_days)} lags...")

    # Step 1: Pre-compute all lag columns
    print(
        f"📋 Pre-computing date/GEOID columns for lags: {min(n_days)} to {max(n_days)}"
    )
    hrs_with_lags = HRSContextLinker.prepare_lag_columns_batch(
        hrs_data, n_days, geoid_prefix
    )

    # Step 2: Extract unique GEOIDs
    unique_geoids = extract_unique_geoids(hrs_with_lags, geoid_prefix)
    print(f"🔍 Extracted {len(unique_geoids)} unique GEOIDs from all lag columns")

    # Step 3: Compute required years and load filtered contextual data
    max_lag = max(n_days)
    required_years = compute_required_years(hrs_data, max_lag)
    available_years = set(contextual_dir.list_years())
    years_to_load = [str(y) for y in required_years if str(y) in available_years]
    print(f"📅 Loading years: {years_to_load}")

    # Set filter and preload
    contextual_dir.geoid_filter = unique_geoids
    contextual_dir.preload_years(years_to_load)

    # Concatenate all years
    print(f"🔗 Concatenating filtered contextual data...")
    contextual_df = pd.concat([contextual_dir[yr].df for yr in years_to_load], axis=0)
    print(f"  Contextual data shape: {contextual_df.shape}")

    # Step 4: Process lags in parallel using threads (shares memory)
    print(f"⚡ Processing {len(n_days)} lags in parallel...")
    temp_files = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(
                process_single_lag,
                n=n,
                hrs_data=hrs_data,
                contextual_dir=contextual_dir,
                id_col=id_col,
                temp_dir=temp_dir,
                prefix=prefix,
                include_lag_date=include_lag_date,
                file_format=file_format,
                geoid_prefix=geoid_prefix,
                precomputed_lag_df=hrs_with_lags,
                preloaded_contextual_df=contextual_df,
            ): n
            for n in n_days
        }

        # Collect results as they complete
        for fut in as_completed(futures):
            n = futures[fut]
            try:
                result = fut.result()
                if result is not None:
                    temp_files.append(result)
            except Exception as e:
                print(f"  ❌ Error processing lag {n}: {e}")

    print(f"✅ Parallel processing complete! Generated {len(temp_files)} files\n")
    return temp_files


def process_single_lag(
    n: int,
    hrs_data: HRSInterviewData,
    contextual_dir: DailyMeasureDataDir,
    id_col: str,
    temp_dir: Path,
    prefix: str = "",
    include_lag_date: bool = False,
    file_format: str = "parquet",
    geoid_prefix: str = "LINKCEN",
    precomputed_lag_df: Optional[pd.DataFrame] = None,
    preloaded_contextual_df: Optional[pd.DataFrame] = None,
) -> Optional[Path]:
    """
    Process a single lag n for a given contextual dataset, merge with HRS data,
    and write the resulting lagged column to a temporary file.

    Can optionally accept pre-computed lag columns and pre-loaded contextual data
    for efficiency when called from parallel processing.

    NOTE: For processing multiple lags efficiently, use process_multiple_lags_batch instead.

    Parameters
    ----------
    n : int
        Lag (in days) to process.
    hrs_data : HRSInterviewData
        HRS interview or epigenetic data object.
    contextual_dir : DailyMeasureDataDir
        Contextual dataset directory (e.g., daily heat, PM2.5, ozone).
    id_col : str
        Unique identifier column for joining (e.g., "hhidpn").
    temp_dir : Path
        Temporary directory to save output files.
    prefix : str, optional
        Optional prefix to add to the output filename (e.g., "heat", "pm25").
    include_lag_date : bool, default False
        Whether to include the lagged date column in the output.
    file_format : {"parquet", "feather", "csv"}, default "parquet"
        File format for the temporary output file.
    geoid_prefix : str, default "LINKCEN"
        Prefix for GEOID column names.
    precomputed_lag_df : pd.DataFrame, optional
        Pre-computed DataFrame with date and GEOID columns. If provided, skips computation.
    preloaded_contextual_df : pd.DataFrame, optional
        Pre-loaded contextual data. If provided, skips loading from contextual_dir.

    Returns
    -------
    Path or None
        Path to the written temporary file, or None if no data was produced
        (e.g., if all geoid values were NA for this lag).
    """
    from .hrs import HRSContextLinker

    try:
        # If pre-computed data is provided, use it directly
        if precomputed_lag_df is not None and preloaded_contextual_df is not None:
            out_df = HRSContextLinker.output_merged_columns(
                hrs_data,
                contextual_dir,
                n=n,
                id_col=id_col,
                precomputed_lag_df=precomputed_lag_df,
                preloaded_contextual_df=preloaded_contextual_df,
                include_lag_date=include_lag_date,
                geoid_prefix=geoid_prefix,
            )
        else:
            # Compute lag columns for this single lag
            hrs_with_lag = HRSContextLinker.prepare_lag_columns_batch(
                hrs_data, [n], geoid_prefix
            )

            # Extract unique GEOIDs for this lag
            unique_geoids = extract_unique_geoids(hrs_with_lag, geoid_prefix)

            # Compute required years and load filtered data
            required_years = compute_required_years(hrs_data, n)
            available_years = set(contextual_dir.list_years())
            years_to_load = [
                str(y) for y in required_years if str(y) in available_years
            ]

            # Set filter and load
            contextual_dir.geoid_filter = unique_geoids
            contextual_dir.preload_years(years_to_load)
            contextual_df = pd.concat(
                [contextual_dir[yr].df for yr in years_to_load], axis=0
            )

            # Merge
            out_df = HRSContextLinker.output_merged_columns(
                hrs_data,
                contextual_dir,
                n=n,
                id_col=id_col,
                precomputed_lag_df=hrs_with_lag,
                preloaded_contextual_df=contextual_df,
                include_lag_date=include_lag_date,
                geoid_prefix=geoid_prefix,
            )

        # If only ID column (no valid merged values), skip
        if out_df.shape[1] <= 1:
            return None

        filename = f"{prefix}_lag_{n:04d}.{file_format}"
        temp_file = temp_dir / filename

        if file_format == "parquet":
            out_df.to_parquet(temp_file, index=False)
        elif file_format == "feather":
            out_df.reset_index(drop=True).to_feather(temp_file)
        elif file_format == "csv":
            out_df.to_csv(temp_file, index=False)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        return temp_file

    except Exception as e:
        print(f"❌ Error processing lag {n} ({prefix}): {e}")
        return None
