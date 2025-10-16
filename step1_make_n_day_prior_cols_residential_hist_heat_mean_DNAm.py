from __future__ import annotations
from pathlib import Path
from tqdm import tqdm

from linkdata.hrs import HRSEpigenetics, ResidentialHistoryHRS, HRSContextLinker
from linkdata.daily_measure import DailyMeasureDataDir  # unified daily measure class

# ---------------------------------------------------------------------
# 📁 File paths
# ---------------------------------------------------------------------
hrs_data_dir = Path(
    r"C:\Users\BioDem\Documents\BioDem\Users\Choi_ec\Heat Epigenetic Age\PrepData"
)
hrs_filename = "HRSprep.dta"  # prepared HRS file name

save_dir = Path(
    r"C:\Users\BioDem\Documents\BioDem\Users\Choi_ec\Heat Epigenetic Age\Donotcopy"
)
save_filename = "HRSHeatMeanLinked.dta"

residential_hist_filename = Path(
    r"C:\Users\BioDem\Documents\BioDem\Data\CDR Data\Residential History\4. reshist 1992_2018 moving month\_ALL1992_2018_reshit_long_mvdate.dta"
)

heat_data_dir = Path(
    r"C:\Users\BioDem\Documents\BioDem\Users\Nam\Linkage\data\daily_heat_mean_long"
)

# ---------------------------------------------------------------------
# 🏠 Residential history + HRS data
# ---------------------------------------------------------------------
print("📥 Loading residential history...")
residential_hist = ResidentialHistoryHRS(residential_hist_filename)

print("📥 Loading HRS epigenetic data...")
hrs_epi_data = HRSEpigenetics(
    hrs_data_dir / hrs_filename,
    datecol="bcdate",
    move=True,
    residential_hist=residential_hist,
)

# ---------------------------------------------------------------------
# 🌡 Daily heat data (lazy load by year)
# ---------------------------------------------------------------------
heat_data_all = DailyMeasureDataDir(
    heat_data_dir, measure_type="heatindex"  # or whatever prefix your heat files use
)

# ---------------------------------------------------------------------
# 🔁 Link over n-day lags
# ---------------------------------------------------------------------
# Up to 2191 days prior (~6 years)
for n in tqdm(range(2191), desc="Linking lags"):
    # 1. Create lagged date column
    n_day_colname = HRSContextLinker.make_n_day_prior_cols(hrs_epi_data, n)

    # 2. Create lagged geoid column (returns NA for dates before first move)
    n_day_geoid_colname = HRSContextLinker.make_geoid_day_prior(
        hrs_epi_data, n_day_colname
    )

    # ⚡ Skip merge if this geoid column has only NA (no valid residences)
    if hrs_epi_data.df[n_day_geoid_colname].isna().all():
        continue

    # 3. Merge with daily heat data by year × geoid
    hrs_epi_data = HRSContextLinker.merge_with_contextual_data(
        hrs_epi_data,
        heat_data_all,
        left_on=[n_day_colname, n_day_geoid_colname],
        drop_left=False,  # keep lag date & geoid for tracking
    )

    # Optional periodic saving every 10 lags
    if n % 10 == 0 and n > 0:
        print(f"💾 Saving after {n} lags...")
        hrs_epi_data.save(save_dir / save_filename)

# ---------------------------------------------------------------------
# 💾 Final save
# ---------------------------------------------------------------------
print("💾 Saving final dataset...")
hrs_epi_data.save(save_dir / save_filename)
print("✅ Done.")
