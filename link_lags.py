#!/usr/bin/env python
"""
Lagged contextual data linkage for HRS datasets.

This script links daily contextual datasets (e.g., heat index, PM2.5)
to HRS interview/epigenetic data by computing n-day prior dates and GEOIDs,
then merging for each lag day. Supports both parallel and sequential processing.

Example:
--------
python link_lags.py \
    --hrs-data "C:/path/to/HRSprep2016full.dta" \
    --context-dir "C:/path/to/daily_heat_long" \
    --output "C:/path/to/output/HRSHeatLinked.dta" \
    --id-col hhidpn \
    --date-col iwdate \
    --measure-type heat_index \
    --data-col HeatIndex \
    --n-lags 2191 \
    --parallel
"""

import argparse
import tempfile
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm

from linkdata.hrs import HRSInterviewData, ResidentialHistoryHRS
from linkdata.daily_measure import DailyMeasureDataDir
from linkdata.process import process_single_lag


# -------------------------------------------------------------------
# 🚀 Main pipeline
# -------------------------------------------------------------------
def main(args: argparse.Namespace):
    hrs_path = Path(args.hrs_data)
    context_dir = Path(args.context_dir)
    out_path = Path(args.output)

    if not hrs_path.exists():
        raise FileNotFoundError(f"HRS file not found: {hrs_path}")
    if not context_dir.exists():
        raise FileNotFoundError(f"Contextual data directory not found: {context_dir}")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------------
    # 🏠 Load residential history (optional)
    # -------------------------------------------------------------------
    if args.residential_hist:
        print("📥 Loading residential history...")
        residential_hist = ResidentialHistoryHRS(Path(args.residential_hist))
    else:
        residential_hist = None

    # -------------------------------------------------------------------
    # 🧠 Load HRS data
    # -------------------------------------------------------------------
    print("📥 Loading HRS interview data...")
    hrs_epi_data = HRSInterviewData(
        hrs_path,
        datecol=args.date_col,
        move=bool(residential_hist),
        residential_hist=residential_hist,
    )

    # -------------------------------------------------------------------
    # 🌡 Load contextual data
    # -------------------------------------------------------------------
    print(f"📥 Loading contextual daily data ({args.measure_type})...")
    contextual_data_all = DailyMeasureDataDir(
        context_dir,
        measure_type=args.measure_type,
        column_name_to_choose=args.data_col,
    )

    # -------------------------------------------------------------------
    # ⚡ Process lags (parallel or sequential)
    # -------------------------------------------------------------------
    temp_dir = Path(tempfile.mkdtemp(prefix="hrs_lag_"))
    print(f"⚡ Temporary files will be written to: {temp_dir}")

    temp_files = []

    if args.parallel:
        # 🧠 Parallel processing
        with ProcessPoolExecutor() as executor:
            futures = {
                executor.submit(
                    process_single_lag,
                    n,
                    args.id_col,
                    temp_dir,
                    hrs_epi_data,
                    contextual_data_all,
                ): n
                for n in range(args.n_lags)
            }
            for fut in tqdm(
                as_completed(futures), total=len(futures), desc="Processing lags"
            ):
                result = fut.result()
                if result is not None:
                    temp_files.append(result)
    else:
        # 🐢 Sequential processing
        for n in tqdm(range(args.n_lags), desc="Processing lags"):
            result = process_single_lag(
                n,
                args.id_col,
                temp_dir,
                hrs_epi_data,
                contextual_data_all,
            )
            if result is not None:
                temp_files.append(result)

    print(f"✅ Finished processing {len(temp_files)} lag files")

    # -------------------------------------------------------------------
    # 🧱 Merge all lag outputs
    # -------------------------------------------------------------------
    print("📎 Merging all lag outputs with main HRS data...")
    final_df = hrs_epi_data.df.copy()

    for f in tqdm(temp_files, desc="Merging parquet files"):
        lag_df = pd.read_parquet(f)
        final_df = final_df.merge(lag_df, on=args.id_col, how="left")

    # -------------------------------------------------------------------
    # 💾 Save final dataset
    # -------------------------------------------------------------------
    print(f"💾 Saving final dataset to {out_path}")
    final_df.to_stata(out_path)
    print("✅ Done.")


# -------------------------------------------------------------------
# 📝 CLI
# -------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Link daily contextual data to HRS dataset with n-day lags."
    )
    parser.add_argument("--hrs-data", required=True, help="Path to HRS Stata file")
    parser.add_argument(
        "--context-dir",
        required=True,
        help="Directory containing daily contextual CSV files",
    )
    parser.add_argument("--output", required=True, help="Output .dta file path")
    parser.add_argument(
        "--id-col",
        required=True,
        help="Unique identifier column name (survey participants)",
    )
    parser.add_argument(
        "--date-col", required=True, help="Interview date column name (survey)"
    )
    parser.add_argument(
        "--measure-type",
        required=True,
        help="Measurement type (e.g., heat_index, pm25, ozone, other contextual data). File name must include this as substrings",
    )
    parser.add_argument(
        "--data-col",
        help="Explicit data column name to use (optional, overrides measure type)",
    )
    parser.add_argument(
        "--residential-hist", help="Path to residential history file (optional)"
    )
    parser.add_argument(
        "--n-lags",
        type=int,
        default=365,
        help="Number of lags to process (default: 365)",
    )
    parser.add_argument(
        "--parallel", action="store_true", help="Use parallel processing"
    )
    args = parser.parse_args()

    main(args)
