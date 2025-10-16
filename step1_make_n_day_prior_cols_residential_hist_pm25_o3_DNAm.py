from pathlib import Path
from tqdm import tqdm
import pandas as pd

from linkdata.hrs import HRSInterviewData, ResidentialHistoryHRS, HRSContextLinker
from linkdata.daily_measure import DailyMeasureDataDir

# -------------------------------------------------------------------
# 📁 File paths
# -------------------------------------------------------------------
BASE_DIR = Path(r"C:\Users\BioDem\Documents\BioDem\Users\Choi_ec\Heat Epigenetic Age")
hrs_data_path = BASE_DIR / "PrepData" / "HRSprep.dta"

save_path = BASE_DIR / "Donotcopy" / "HRSPM25O3Linked.dta"

residential_hist_path = Path(
    r"C:\Users\BioDem\Documents\BioDem\Data\CDR Data\Residential History\4. reshist 1992_2018 moving month\_ALL1992_2018_reshit_long_mvdate.dta"
)

pm25_dir = Path(r"C:\Users\BioDem\Documents\BioDem\Users\Nam\Linkage\data\PM25")
o3_dir = Path(r"C:\Users\BioDem\Documents\BioDem\Users\Nam\Linkage\data\O3")

# -------------------------------------------------------------------
# 📝 Column rename dictionaries
# -------------------------------------------------------------------
# If the same rename applies to all years, we wrap it as {year: dict} automatically inside DailyMeasureDataDir
pm25_rename_cols = {"date": "Date", "fips": "GEOID10", "pm25_daily_averageugm3": "pm25"}
o3_rename_cols = {"Date": "Date", "Loc_Label1": "GEOID10", "Prediction": "o3"}

# -------------------------------------------------------------------
# 🌫 Load daily measure directories
# -------------------------------------------------------------------
pm25_data_all = DailyMeasureDataDir(
    dir_name=pm25_dir,
    measure_type="pm25",
    rename_col_dict=(
        None
        if not pm25_rename_cols
        else {yr: pm25_rename_cols for yr in range(1992, 2020)}
    ),
)

o3_data_all = DailyMeasureDataDir(
    dir_name=o3_dir,
    measure_type="ozone",
    rename_col_dict=(
        None if not o3_rename_cols else {yr: o3_rename_cols for yr in range(1992, 2020)}
    ),
)

# -------------------------------------------------------------------
# 🏠 Load HRS data & residential history
# -------------------------------------------------------------------
print("📥 Loading residential history...")
residential_hist = ResidentialHistoryHRS(residential_hist_path)

print("📥 Loading HRS interview data...")
hrs_epi_data = HRSInterviewData(
    hrs_data_path,
    datecol="bcdate",
    move=True,
    residential_hist=residential_hist,
)

# -------------------------------------------------------------------
# 🔁 Main linkage loop
# -------------------------------------------------------------------
N_LAGS = 366
ID_COL = "hhidpn"

print(f"🔗 Linking {N_LAGS} daily lag columns for PM2.5 and O₃ ...")

for n in tqdm(range(N_LAGS), desc="Lag processing"):
    # PM2.5
    pm25_df = HRSContextLinker.output_merged_columns(
        hrs_epi_data,
        pm25_data_all,
        n=n,
        id_col=ID_COL,
        include_lag_date=False,
    )

    # O₃
    o3_df = HRSContextLinker.output_merged_columns(
        hrs_epi_data,
        o3_data_all,
        n=n,
        id_col=ID_COL,
        include_lag_date=False,
    )

    # Merge both new columns into main df
    hrs_epi_data.df = hrs_epi_data.df.merge(pm25_df, on=ID_COL, how="left")
    hrs_epi_data.df = hrs_epi_data.df.merge(o3_df, on=ID_COL, how="left")

    # Periodic saving
    if n % 10 == 0 and n > 0:
        print(f"💾 Saving intermediate results after lag {n} ...")
        hrs_epi_data.save(save_path)

# -------------------------------------------------------------------
# 💾 Final save
# -------------------------------------------------------------------
print("✅ Saving final dataset...")
hrs_epi_data.save(save_path)
print(f"🏁 Done. Linked dataset written to: {save_path}")
