from pathlib import Path
from typing import Optional
import pandas as pd
from .hrs import HRSInterviewData, HRSContextLinker
from .daily_measure import DailyMeasureDataDir


def process_single_lag(
    n: int,
    hrs_data: HRSInterviewData,
    contextual_dir: DailyMeasureDataDir,
    id_col: str,
    temp_dir: Path,
    prefix: str = "",
    include_lag_date: bool = False,
    file_format: str = "parquet",
) -> Optional[Path]:
    """
    Process a single lag n for a given contextual dataset, merge with HRS data,
    and write the resulting lagged column to a temporary file.

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

    Returns
    -------
    Path or None
        Path to the written temporary file, or None if no data was produced
        (e.g., if all geoid values were NA for this lag).
    """
    try:
        out_df = HRSContextLinker.output_merged_columns(
            hrs_data,
            contextual_dir,
            n=n,
            id_col=id_col,
            include_lag_date=include_lag_date,
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
