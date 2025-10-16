from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
from tqdm import tqdm

from .daily_measure import DailyMeasureDataDir  # unified DailyMeasure classes


# ---------------------------------------------------------------------
# 1. ResidentialHistoryHRS
# ---------------------------------------------------------------------
class ResidentialHistoryHRS:
    """
    Parses respondent-level residential move history from HRS geocoded data,
    and enables date-based GEOID lookup for linkage with contextual datasets.
    """

    def __init__(
        self,
        filename: Union[str, Path],
        hhidpn: str = "hhidpn",
        movecol: str = "trmove_tr",
        mvyear: str = "mvyear",
        mvmonth: str = "mvmonth",
        moved_mark: str = "1. move",
        geoid: str = "LINKCEN2010",
        survey_yr_col: str = "year",
        first_tract_mark: float = 999.0,
    ):
        self.filename = Path(filename)
        self.hhidpn = hhidpn
        self.movecol = movecol
        self.mvyear = mvyear
        self.mvmonth = mvmonth
        self.moved_mark = moved_mark
        self.geoid = geoid
        self.survey_yr_col = survey_yr_col
        self.first_tract_mark = first_tract_mark

        # Load only once (Stata read can be expensive)
        self.df = pd.read_stata(self.filename)
        self._move_info = self._parse_move_info()

    def _parse_move_info(self) -> Dict[str, tuple[list[pd.Timestamp], list[str]]]:
        """
        Builds a dict mapping hhidpn → (list of move dates, list of corresponding GEOIDs)
        """
        print("📌 Parsing residential move history...")
        move_info = {}
        for pid, df_person in tqdm(self.df.groupby(self.hhidpn)):
            dates, geoids = [], []

            # First tract
            first_rows = df_person[df_person[self.movecol] == self.first_tract_mark]
            if first_rows.empty:
                continue
            first = first_rows.iloc[0]
            start_dt = pd.to_datetime(f"{int(first[self.survey_yr_col])}-01-01")
            dates.append(start_dt)
            geoids.append(str(first[self.geoid]).zfill(11))

            # Subsequent moves
            moved_rows = df_person[df_person[self.movecol] == self.moved_mark]
            for _, row in moved_rows.iterrows():
                dt = pd.to_datetime(
                    f"{int(row[self.mvyear])}-{int(row[self.mvmonth])}-01"
                )
                dates.append(dt)
                geoids.append(str(row[self.geoid]).zfill(11))

            move_info[pid] = (dates, geoids)

        return move_info

    @staticmethod
    def _find_geoid_for_date(
        dt: pd.Timestamp, move_dates: list[pd.Timestamp], move_geoids: list[str]
    ) -> Optional[str]:
        """Return geoid for dt, or None if dt is earlier than first recorded move."""
        if dt < move_dates[0]:
            return None  # or pd.NA if you prefer pandas NA semantics

        if len(move_dates) == 1:
            return move_geoids[0]

        for i, move_dt in enumerate(move_dates):
            if move_dt > dt:
                return move_geoids[i - 1]

        return move_geoids[-1]

    def create_geoid_based_on_date(
        self, hhidpn_series: pd.Series, date_series: pd.Series
    ) -> pd.Series:
        """
        Returns a Series of GEOIDs aligned with hhidpn_series,
        based on the move history and the provided dates.
        """
        assert len(hhidpn_series) == len(date_series)
        geoids = []
        for pid, dt in zip(hhidpn_series, date_series):
            move_dates, move_geoids = self._move_info[pid]
            geoids.append(self._find_geoid_for_date(dt, move_dates, move_geoids))
        return pd.Series(geoids, index=hhidpn_series.index, dtype="string")


# ---------------------------------------------------------------------
# 2. HRSEpigenetics
# ---------------------------------------------------------------------
class HRSInterviewData:
    """
    Wrapper around survey data with interview (or blood collection date
    for epigenetic biomarker data (e.g., HRS VBS)).
    adding date-based GEOID creation for linkage with contextual data.
    """

    def __init__(
        self,
        filename: Union[str, Path],
        datecol: str = "bcdate",
        move: bool = True,
        residential_hist: Optional[ResidentialHistoryHRS] = None,
        hhidpn: str = "hhidpn",
    ):
        self.filename = Path(filename)
        self.df = pd.read_stata(self.filename)
        self.columns = self.df.columns
        assert datecol in self.columns, f"Date column `{datecol}` not in data!"

        self.datecol = datecol
        self.hhidpn = hhidpn
        self.move = move
        self.residential_hist = residential_hist

        # Format existing GEOIDs
        geoid_cols = [c for c in self.columns if "LINKCEN" in c]
        for col in geoid_cols:
            self.df[col] = self.df[col].astype(str).str.zfill(11)

    def get_geoid_based_on_date(self, date_series: pd.Series) -> pd.Series:
        return self.residential_hist.create_geoid_based_on_date(
            self.df[self.hhidpn], date_series
        )

    def save(self, save_name: Union[str, Path]) -> None:
        self.df.to_stata(save_name)


# ---------------------------------------------------------------------
# 3. HRSContextLinker
# ---------------------------------------------------------------------
from typing import Optional, List
import pandas as pd


class HRSContextLinker:
    """
    Handles temporal/geographic alignment between HRS epigenetic data
    and contextual daily measure data (e.g., heat index, Tmax, PM2.5),
    including:
    - n-day prior date column creation
    - GEOID column assignment based on residential history or static data
    - Single or batch merging with contextual data sources
    - Outputting merged columns for parallel workflows
    """

    # ------------------------------------------------------------------
    # 1. n-day prior date column
    # ------------------------------------------------------------------
    @staticmethod
    def make_n_day_prior_cols(hrs_data: "HRSInterviewData", n_day_prior: int) -> str:
        """
        Create a new column representing the date n days prior to the
        respondent's reference date column.
        """
        colname = f"{hrs_data.datecol}_{n_day_prior}day_prior"
        hrs_data.df[colname] = hrs_data.df[hrs_data.datecol] - pd.to_timedelta(
            n_day_prior, unit="d"
        )
        return colname

    # ------------------------------------------------------------------
    # 2. Geoid assignment for lag date
    # ------------------------------------------------------------------
    @staticmethod
    def make_geoid_day_prior(
        hrs_data: "HRSInterviewData",
        merge_date_col: str,
        geoid_prefix: str = "LINKCEN",
        df: Optional[pd.DataFrame] = None,
    ) -> str:
        """
        Create a geoid column based on a lagged date column.
        If df is provided, operate on that DataFrame instead of hrs_data.df.
        """
        target_df = hrs_data.df if df is None else df
        n_prior_str = "_".join(merge_date_col.split("_")[1:])
        colname = f"{geoid_prefix}_{n_prior_str}"

        if hrs_data.move:
            geoids = hrs_data.get_geoid_based_on_date(target_df[merge_date_col])
            target_df[colname] = geoids
        else:
            # Vectorized lookup from existing static columns
            years = target_df[merge_date_col].dt.year.astype(str)
            col_names = geoid_prefix + "2010_" + years
            col_idx = hrs_data.df.columns.get_indexer(col_names)
            row_idx = pd.RangeIndex(len(target_df))
            geoid_values = hrs_data.df.to_numpy()[row_idx, col_idx]
            target_df[colname] = geoid_values

        return colname

    # ------------------------------------------------------------------
    # 3. Merge HRS with contextual data (single big merge)
    # ------------------------------------------------------------------
    @staticmethod
    def merge_with_contextual_data(
        hrs_data: "HRSInterviewData",
        contextual_dir: "DailyMeasureDataDir",
        left_on: List[str],
        drop_left: bool = True,
    ) -> "HRSInterviewData":
        """
        Merge HRS data with contextual daily data across all years in a single merge.
        This is typically faster than looping year by year.
        """
        date_col = left_on[0]
        nday_prior_str = "_".join(date_col.split("_")[1:])

        # Build one contextual DataFrame from all years
        years = contextual_dir.list_years()
        contextual_df = pd.concat([contextual_dir[yr].df for yr in years], axis=0)
        first_context = contextual_dir[years[0]]
        right_on = [first_context.date_col, first_context.geoid_col]

        # Check for overlapping columns
        overlap = set(hrs_data.df.columns) & set(contextual_df.columns) - set(right_on)
        if overlap:
            raise ValueError(f"Column overlap during merge: {overlap}")

        merged = pd.merge(
            hrs_data.df,
            contextual_df,
            how="left",
            left_on=left_on,
            right_on=right_on,
            suffixes=(None, None),
        )

        # Drop key columns if needed
        merged.drop(right_on, axis=1, inplace=True)
        if drop_left:
            merged.drop(left_on[1:], axis=1, inplace=True)

        # Rename contextual measure column to indicate lag
        data_col = first_context.data_col
        merged.rename(columns={data_col: f"{data_col}_{nday_prior_str}"}, inplace=True)

        hrs_data.df = merged
        return hrs_data

    # ------------------------------------------------------------------
    # 4. Output merged columns for a specific lag (no mutation)
    # ------------------------------------------------------------------
    @staticmethod
    def output_merged_columns(
        hrs_data: "HRSInterviewData",
        contextual_dir: "DailyMeasureDataDir",
        n: int,
        id_col: str,
        include_lag_date: bool = False,
    ) -> pd.DataFrame:
        """
        For a specific lag `n`, compute the n-day prior and geoid columns,
        merge with contextual data, and return only the merged contextual
        column plus ID (and optionally the lag date column).
        This method does not modify hrs_data.df, making it suitable for
        parallelized linkage workflows.
        """
        # Shallow copy of relevant columns
        hrs_copy = hrs_data.df[[id_col, hrs_data.datecol]].copy()

        # n-day prior date column
        n_day_colname = f"{hrs_data.datecol}_{n}day_prior"
        hrs_copy[n_day_colname] = hrs_copy[hrs_data.datecol] - pd.to_timedelta(
            n, unit="d"
        )

        # Geoid column using helper
        n_day_geoid_colname = HRSContextLinker.make_geoid_day_prior(
            hrs_data, n_day_colname, df=hrs_copy
        )

        # If no valid geoid, return empty contextual column
        if hrs_copy[n_day_geoid_colname].isna().all():
            out_cols = [id_col]
            if include_lag_date:
                out_cols.append(n_day_colname)
            return hrs_copy[out_cols]

        # Merge with contextual data
        years = contextual_dir.list_years()
        contextual_df = pd.concat([contextual_dir[yr].df for yr in years], axis=0)
        first_context = contextual_dir[years[0]]
        right_on = [first_context.date_col, first_context.geoid_col]

        merged = pd.merge(
            hrs_copy,
            contextual_df,
            how="left",
            left_on=[n_day_colname, n_day_geoid_colname],
            right_on=right_on,
            suffixes=(None, None),
        )

        # Rename contextual column
        data_col = first_context.data_col
        new_col_name = f"{data_col}_{n_day_colname}"
        merged.rename(columns={data_col: new_col_name}, inplace=True)

        out_cols = [id_col]
        if include_lag_date:
            out_cols.append(n_day_colname)
        out_cols.append(new_col_name)

        return merged[out_cols]
