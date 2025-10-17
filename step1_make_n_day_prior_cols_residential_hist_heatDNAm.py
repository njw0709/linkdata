#!/usr/bin/env python
"""
Lagged heat data linkage for HRS epigenetic age dataset.

This script calls the link_lags.py CLI tool to link heat index data
to HRS interview data with residential history.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

# -------------------------------------------------------------------
# 📁 Configuration
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

# Processing options
id_col = "hhidpn"
date_col = "iwdate"
measure_type = "heat_index"
n_lags = 2191
use_parallel = True  # 👈 Set to True for parallel processing, False for sequential

# -------------------------------------------------------------------
# 🚀 Call link_lags.py CLI tool
# -------------------------------------------------------------------
hrs_path = hrs_data_dir / hrs_filename

cmd = [
    "python",
    "link_lags.py",
    "--hrs-data",
    str(hrs_path),
    "--context-dir",
    str(heat_data_dir),
    "--save-dir",
    str(save_dir),
    "--output_name",
    save_filename,
    "--id-col",
    id_col,
    "--date-col",
    date_col,
    "--measure-type",
    measure_type,
    "--residential-hist",
    str(residential_hist_filename),
    "--n-lags",
    str(n_lags),
]

if use_parallel:
    cmd.append("--parallel")

print("🚀 Running link_lags.py with the following command:")
print(" ".join(cmd))
print()

# Run the command
result = subprocess.run(cmd, check=True)

if result.returncode == 0:
    print("\n✅ Linkage completed successfully!")
else:
    print("\n❌ Linkage failed!")
    exit(result.returncode)
