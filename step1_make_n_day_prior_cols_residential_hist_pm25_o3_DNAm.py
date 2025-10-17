#!/usr/bin/env python
"""
Lagged PM2.5 and O3 data linkage for HRS epigenetic age dataset.

This script calls the link_lags.py CLI tool twice (once for PM2.5, once for O3)
and then merges the results together.

NOTE: The CLI tool doesn't currently support custom column renaming.
Ensure your data files have the correct column names:
- PM2.5: Date, GEOID10, pm25
- O3: Date, GEOID10, o3
"""

from __future__ import annotations

import subprocess
from pathlib import Path
import pandas as pd

# -------------------------------------------------------------------
# 📁 Configuration
# -------------------------------------------------------------------
BASE_DIR = Path(r"C:\Users\BioDem\Documents\BioDem\Users\Choi_ec\Heat Epigenetic Age")
hrs_data_path = BASE_DIR / "PrepData" / "HRSprep.dta"

save_dir = BASE_DIR / "Donotcopy"
save_filename = "HRSPM25O3Linked.dta"

residential_hist_path = Path(
    r"C:\Users\BioDem\Documents\BioDem\Data\CDR Data\Residential History\4. reshist 1992_2018 moving month\_ALL1992_2018_reshit_long_mvdate.dta"
)

pm25_dir = Path(r"C:\Users\BioDem\Documents\BioDem\Users\Nam\Linkage\data\PM25")
o3_dir = Path(r"C:\Users\BioDem\Documents\BioDem\Users\Nam\Linkage\data\O3")

# Processing options
id_col = "hhidpn"
date_col = "bcdate"
n_lags = 366
use_parallel = True  # Set to True for parallel processing, False for sequential

# -------------------------------------------------------------------
# 🚀 Step 1: Link PM2.5 data
# -------------------------------------------------------------------
pm25_output = "HRSPM25Linked.dta"

cmd_pm25 = [
    "python",
    "link_lags.py",
    "--hrs-data",
    str(hrs_data_path),
    "--context-dir",
    str(pm25_dir),
    "--save-dir",
    str(save_dir),
    "--output_name",
    pm25_output,
    "--id-col",
    id_col,
    "--date-col",
    date_col,
    "--measure-type",
    "pm25",
    "--data-col",
    "pm25",
    "--residential-hist",
    str(residential_hist_path),
    "--n-lags",
    str(n_lags),
]

if use_parallel:
    cmd_pm25.append("--parallel")

print("🌫 Running link_lags.py for PM2.5...")
print(" ".join(cmd_pm25))
print()

result_pm25 = subprocess.run(cmd_pm25, check=True)

if result_pm25.returncode != 0:
    print("❌ PM2.5 linkage failed!")
    exit(result_pm25.returncode)

# -------------------------------------------------------------------
# 🚀 Step 2: Link O3 data
# -------------------------------------------------------------------
o3_output = "HRSO3Linked.dta"

cmd_o3 = [
    "python",
    "link_lags.py",
    "--hrs-data",
    str(hrs_data_path),
    "--context-dir",
    str(o3_dir),
    "--save-dir",
    str(save_dir),
    "--output_name",
    o3_output,
    "--id-col",
    id_col,
    "--date-col",
    date_col,
    "--measure-type",
    "ozone",
    "--data-col",
    "o3",
    "--residential-hist",
    str(residential_hist_path),
    "--n-lags",
    str(n_lags),
]

if use_parallel:
    cmd_o3.append("--parallel")

print("\n💨 Running link_lags.py for O3...")
print(" ".join(cmd_o3))
print()

result_o3 = subprocess.run(cmd_o3, check=True)

if result_o3.returncode != 0:
    print("❌ O3 linkage failed!")
    exit(result_o3.returncode)

# -------------------------------------------------------------------
# 🧱 Step 3: Merge PM2.5 and O3 results
# -------------------------------------------------------------------
print("\n📎 Merging PM2.5 and O3 datasets...")

pm25_path = save_dir / pm25_output
o3_path = save_dir / o3_output
final_path = save_dir / save_filename

# Load both datasets
pm25_data = pd.read_stata(pm25_path)
o3_data = pd.read_stata(o3_path)

# Get column lists (excluding id_col)
pm25_cols = [col for col in pm25_data.columns if col != id_col]
o3_cols = [col for col in o3_data.columns if col != id_col]

# Get only the new lag columns (assuming original HRS columns are the same in both)
# We'll use PM2.5 as base and merge O3 lag columns
pm25_lag_cols = [col for col in pm25_cols if "pm25" in col.lower()]
o3_lag_cols = [col for col in o3_cols if "o3" in col.lower() or "ozone" in col.lower()]

# Select columns to merge
o3_to_merge = o3_data[[id_col] + o3_lag_cols]

# Merge
final_df = pm25_data.merge(o3_to_merge, on=id_col, how="left")

# Save final dataset
print(f"💾 Saving merged dataset to {final_path}")
final_df.to_stata(final_path)

print(f"\n✅ Linkage completed successfully!")
print(f"📊 Final dataset shape: {final_df.shape}")
print(f"🏁 Done. Linked dataset written to: {final_path}")
