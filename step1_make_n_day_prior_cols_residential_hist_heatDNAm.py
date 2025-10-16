from __future__ import annotations

from pathlib import Path
from tqdm import tqdm
import pandas as pd

from linkdata.hrs import HRSInterviewData, ResidentialHistoryHRS
from linkdata.daily_measure import DailyMeasureDataDir
from linkdata.process import process_multiple_lags_batch, process_multiple_lags_parallel

# -------------------------------------------------------------------
# 📁 File paths
# -------------------------------------------------------------------
hrs_data_dir = Path(
    r"C:\Users\BioDem\Documents\BioDem\Users\Choi_ec\Heat Epigenetic Age\PrepData"
)
hrs_filename = "HRSprep2016full.dta"

save_dir = Path(
    r"C:\Users\BioDem\Documents\BioDem\Users\Choi_ec\Heat Epigenetic Age\Donotcopy"
)
save_filename = "HRSHeatOriginalLinkedFullSample.dta"

residential_hist_filename = Path(
    r"C:\Users\BioDem\Documents\BioDem\Data\CDR Data\Residential History\4. reshist 1992_2018 moving month\_ALL1992_2018_reshit_long_mvdate.dta"
)

heat_data_dir = Path(
    r"C:\Users\BioDem\Documents\BioDem\Users\Nam\Linkage\data\daily_heat_long"
)

# -------------------------------------------------------------------
# 🏠 Load data
# -------------------------------------------------------------------
print("📥 Loading residential history...")
residential_hist = ResidentialHistoryHRS(residential_hist_filename)

print("📥 Loading HRS interview data...")
hrs_epi_data = HRSInterviewData(
    hrs_data_dir / hrs_filename,
    datecol="iwdate",
    move=True,
    residential_hist=residential_hist,
)

print("📥 Initializing heat data...")
heat_data_all = DailyMeasureDataDir(heat_data_dir, measure_type="heat_index")

# -------------------------------------------------------------------
# ⚡ Optimized processing with GEOID filtering and parallelization
# -------------------------------------------------------------------
# 🎯 NEW: Uses process_multiple_lags_batch or process_multiple_lags_parallel:
#   1. Pre-computes all date/GEOID columns at once
#   2. Extracts unique GEOIDs and filters heat data (massive I/O reduction)
#   3. Loads filtered data once and reuses for all lags
#   4. Processes lags efficiently (sequential or parallel)
# -------------------------------------------------------------------
id_col = "hhidpn"
n_lags = 2191
use_parallel = True  # 👈 Set to True for parallel processing, False for sequential

temp_dir = Path(save_dir) / "temp_lag_files"
temp_dir.mkdir(parents=True, exist_ok=True)
print(f"⚡ Temporary lag files will be saved to: {temp_dir}")

# Generate list of lags to process
lags_to_process = list(range(n_lags))

if use_parallel:
    print(
        f"🚀 Using optimized PARALLEL processing with GEOID filtering for {n_lags} lags"
    )
    print("   This will:")
    print("   1. Pre-compute all lag date/GEOID columns")
    print("   2. Extract unique GEOIDs from your HRS data")
    print("   3. Load only relevant heat data (99%+ I/O reduction)")
    print("   4. Process all lags in parallel threads (shared memory)")

    # Call parallel processing function
    temp_files = process_multiple_lags_parallel(
        hrs_data=hrs_epi_data,
        contextual_dir=heat_data_all,
        n_days=lags_to_process,
        id_col=id_col,
        temp_dir=temp_dir,
        prefix="heat",
        include_lag_date=False,
        file_format="parquet",
        max_workers=None,  # Uses default (usually CPU count * 5)
    )
else:
    print(
        f"🐢 Using optimized SEQUENTIAL processing with GEOID filtering for {n_lags} lags"
    )
    print("   This will:")
    print("   1. Pre-compute all lag date/GEOID columns")
    print("   2. Extract unique GEOIDs from your HRS data")
    print("   3. Load only relevant heat data (99%+ I/O reduction)")
    print("   4. Process all lags sequentially")

    # Call batch processing function
    temp_files = process_multiple_lags_batch(
        hrs_data=hrs_epi_data,
        contextual_dir=heat_data_all,
        n_days=lags_to_process,
        id_col=id_col,
        temp_dir=temp_dir,
        prefix="heat",
        include_lag_date=False,
        file_format="parquet",
    )

print(f"✅ Finished processing {len(temp_files)} lag files")

# -------------------------------------------------------------------
# 🧱 Final merge
# -------------------------------------------------------------------
print("📎 Merging all lag outputs with main HRS data...")
final_df = hrs_epi_data.df.copy()

for f in tqdm(temp_files, desc="Merging parquet files"):
    lag_df = pd.read_parquet(f)
    final_df = final_df.merge(lag_df, on=id_col, how="left")

# -------------------------------------------------------------------
# 💾 Save final dataset
# -------------------------------------------------------------------
save_path = save_dir / save_filename
final_df.to_stata(save_path)
print(f"💾 Final dataset saved to {save_path}")
print(f"📊 Final dataset shape: {final_df.shape}")
