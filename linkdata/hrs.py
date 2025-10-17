from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
from tqdm import tqdm

from .daily_measure import DailyMeasureDataDir
from .io_utils import read_data, write_data


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

        # Load only once (file read can be expensive)
        self.df = read_data(self.filename)
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
        geoid_prefix: str = "LINKCEN",
    ):
        self.filename = Path(filename)
        self.df = read_data(self.filename)
        self.columns = self.df.columns
        assert datecol in self.columns, f"Date column `{datecol}` not in data!"

        self.datecol = datecol
        self.hhidpn = hhidpn
        self.move = move
        self.residential_hist = residential_hist
        self.geoid_prefix = geoid_prefix

        # Format existing GEOIDs
        geoid_cols = [c for c in self.columns if geoid_prefix in c]
        for col in geoid_cols:
            self.df[col] = self.df[col].astype(str).str.zfill(11)

    def get_geoid_based_on_date(self, date_series: pd.Series) -> pd.Series:
        return self.residential_hist.create_geoid_based_on_date(
            self.df[self.hhidpn], date_series
        )

    def save(self, save_name: Union[str, Path]) -> None:
        write_data(self.df, save_name)


# ---------------------------------------------------------------------
# 3. HRSContextLinker
# ---------------------------------------------------------------------


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
    # 1b. Batch column preparation
    # ------------------------------------------------------------------
    @staticmethod
    def prepare_lag_columns_batch(
        hrs_data: "HRSInterviewData",
        n_days: List[int],
        geoid_prefix: str = "LINKCEN",
    ) -> pd.DataFrame:
        """
        Pre-create all n-day-prior date and GEOID columns for multiple lags.
        Returns DataFrame with ID, original date, and all lag date/GEOID columns.

        This is more efficient than creating columns one at a time because:
        - All date columns are vectorized operations on the same base date
        - All GEOID lookups happen in a single pass
        - Results can be reused for multiple merges

        Parameters
        ----------
        hrs_data : HRSInterviewData
            HRS interview or epigenetic data object
        n_days : List[int]
            List of lag periods (in days) to create columns for
        geoid_prefix : str, default "LINKCEN"
            Prefix for GEOID column names

        Returns
        -------
        pd.DataFrame
            DataFrame with all original columns plus date/GEOID columns for each lag
        """
        # Start with copy of HRS data
        result_df = hrs_data.df.copy()

        # Collect all new columns to avoid fragmentation
        new_columns = {}

        # Create date columns for all lags
        for n in n_days:
            date_colname = f"{hrs_data.datecol}_{n}day_prior"
            new_columns[date_colname] = result_df[hrs_data.datecol] - pd.to_timedelta(
                n, unit="d"
            )

        # Create GEOID columns for all lags using the helper method
        for n in n_days:
            date_colname = f"{hrs_data.datecol}_{n}day_prior"
            n_prior_str = "_".join(date_colname.split("_")[1:])
            geoid_colname = f"{geoid_prefix}_{n_prior_str}"

            # Use helper method to compute GEOIDs
            new_columns[geoid_colname] = HRSContextLinker._compute_geoid_for_date(
                hrs_data, new_columns[date_colname], geoid_prefix
            )

        # Concatenate all new columns at once to avoid fragmentation
        new_cols_df = pd.DataFrame(new_columns, index=result_df.index)
        result_df = pd.concat([result_df, new_cols_df], axis=1)

        return result_df

    # ------------------------------------------------------------------
    # 2. Geoid assignment for lag date
    # ------------------------------------------------------------------
    @staticmethod
    def _compute_geoid_for_date(
        hrs_data: "HRSInterviewData",
        date_series: pd.Series,
        geoid_prefix: str = "LINKCEN",
    ) -> pd.Series:
        """
        Compute GEOID values for a given date series.

        Returns the GEOID Series without modifying any DataFrame.
        """
        if hrs_data.move:
            # Use residential history for dynamic lookup
            geoids = hrs_data.get_geoid_based_on_date(date_series)
        else:
            # Vectorized lookup from existing static columns
            years = date_series.dt.year.astype(str)
            col_names = geoid_prefix + "2010_" + years
            col_idx = hrs_data.df.columns.get_indexer(col_names)
            row_idx = pd.RangeIndex(len(date_series))
            geoids = pd.Series(
                hrs_data.df.to_numpy()[row_idx, col_idx],
                index=date_series.index,
                dtype="string",
            )
        return geoids

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

        # Compute GEOIDs using helper method
        geoids = HRSContextLinker._compute_geoid_for_date(
            hrs_data, target_df[merge_date_col], geoid_prefix
        )
        target_df[colname] = geoids

        return colname

    # ------------------------------------------------------------------
    # 3. Merge HRS with contextual data (single big merge)
    # ------------------------------------------------------------------
    @staticmethod
    def merge_with_contextual_data(
        hrs_data: "HRSInterviewData",
        contextual_dir: DailyMeasureDataDir,
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
        contextual_dir: DailyMeasureDataDir,
        n: int,
        id_col: str,
        precomputed_lag_df: pd.DataFrame,
        preloaded_contextual_df: pd.DataFrame,
        include_lag_date: bool = False,
        geoid_prefix: str = "LINKCEN",
    ) -> pd.DataFrame:
        """
        For a specific lag n, merge pre-computed lag columns with pre-loaded contextual data.

        This method expects pre-computed date/GEOID columns and pre-loaded contextual data
        for efficiency. Use with process_multiple_lags_batch for best performance.

        Parameters
        ----------
        hrs_data : HRSInterviewData
            HRS interview or epigenetic data object (used for metadata like datecol)
        contextual_dir : DailyMeasureDataDir
            Directory containing contextual daily measure data (used for metadata)
        n : int
            Lag period in days
        id_col : str
            Unique identifier column for joining (e.g., "hhidpn")
        precomputed_lag_df : pd.DataFrame
            Pre-computed DataFrame with date and GEOID columns for all lags.
            Should contain: id_col, {datecol}_{n}day_prior, {geoid_prefix}_{n}day_prior
        preloaded_contextual_df : pd.DataFrame
            Pre-loaded and filtered contextual data (already concatenated across years)
        include_lag_date : bool, default False
            Whether to include the lagged date column in the output
        geoid_prefix : str, default "LINKCEN"
            Prefix for GEOID column names

        Returns
        -------
        pd.DataFrame
            DataFrame with ID, optionally lag date, and merged contextual column
        """
        # Extract pre-computed lag columns
        n_day_colname = f"{hrs_data.datecol}_{n}day_prior"
        n_day_geoid_colname = f"{geoid_prefix}_{n}day_prior"

        hrs_copy = precomputed_lag_df[
            [id_col, n_day_colname, n_day_geoid_colname]
        ].copy()

        # If no valid geoid, return empty contextual column
        if hrs_copy[n_day_geoid_colname].isna().all():
            out_cols = [id_col]
            if include_lag_date:
                out_cols.append(n_day_colname)
            return hrs_copy[out_cols]

        # Use pre-loaded contextual data
        contextual_df = preloaded_contextual_df
        # Get metadata from contextual_dir
        first_year = contextual_dir.list_years()[0]
        first_context = contextual_dir[first_year]
        right_on = [first_context.date_col, first_context.geoid_col]

        # Merge
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
