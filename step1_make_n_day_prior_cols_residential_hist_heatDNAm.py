from __future__ import annotations

from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
import tempfile

from linkdata.hrs import HRSInterviewData, ResidentialHistoryHRS
from linkdata.daily_measure import DailyMeasureDataDir
from linkdata.process import process_single_lag

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
# ⚡ Parallel or sequential processing
# -------------------------------------------------------------------
id_col = "hhidpn"
n_lags = 2191
use_parallel = True  # 👈 Toggle this to False for sequential processing

temp_dir = Path(tempfile.mkdtemp(prefix="hrs_lag_parallel_"))
print(f"⚡ Temporary files will be written to: {temp_dir}")

temp_files: list[Path] = []

if use_parallel:
    print("🚀 Using parallel processing")
    with ProcessPoolExecutor() as executor:
        futures = {
            executor.submit(
                process_single_lag,
                n=n,
                hrs_data=hrs_epi_data,
                contextual_dir=heat_data_all,
                id_col=id_col,
                temp_dir=temp_dir,
                prefix="heat",
                include_lag_date=False,
                file_format="parquet",
            ): n
            for n in range(n_lags)
        }

        for fut in tqdm(
            as_completed(futures), total=len(futures), desc="Processing lags"
        ):
            result = fut.result()
            if result is not None:
                temp_files.append(result)
else:
    print("🐢 Using sequential processing")
    for n in tqdm(range(n_lags), desc="Processing lags"):
        result = process_single_lag(
            n=n,
            hrs_data=hrs_epi_data,
            contextual_dir=heat_data_all,
            id_col=id_col,
            temp_dir=temp_dir,
            prefix="heat",
            include_lag_date=False,
            file_format="parquet",
        )
        if result is not None:
            temp_files.append(result)

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
