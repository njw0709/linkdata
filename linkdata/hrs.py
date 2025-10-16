import pandas as pd
from .heat_index import HeatIndexDataDir
from typing import Union
from tqdm import tqdm

class ResidentialHistoryHRS:
    def __init__(
        self, 
        filename:str, 
        hhidpn:str = "hhidpn", 
        movecol:str = "trmove_tr", 
        mvyear: str = "mvyear", 
        mvmonth:str="mvmonth", 
        moved_mark: str = "1. move", 
        geoid: str = "LINKCEN2010", 
        survey_yr_col: str= "year", 
        first_tract_mark:float = 999.0
    ):
        self.filename = filename
        self.hhidpn = hhidpn
        self.movecol = movecol
        self.mvyear = mvyear
        self.mvmonth = mvmonth
        self.moved_mark = moved_mark
        self.df = pd.read_stata(filename)
        self.geoid = geoid
        self.survey_yr_col = survey_yr_col
        self.first_tract_mark = first_tract_mark
        self.move_info_dict = self._parse_move_info()


    def _parse_move_info(self):
        print("parsing move info...")
        all_pplid = self.df[self.hhidpn].unique()
        move_info = {}
        for hhidpn in tqdm(all_pplid):
            move_data_person = self.df[self.df[self.hhidpn] == hhidpn]
            move_dates = []
            move_geoids = []
            first_row = move_data_person[move_data_person[self.movecol] == self.first_tract_mark]
            if len(first_row) == 0:
                continue
            else:
                first_row = first_row.iloc[0]
            dt_str = "{}-01-01".format(int(first_row[self.survey_yr_col]))
            dt = pd.to_datetime(dt_str)
            move_dates.append(dt)
            move_geoids.append(first_row[self.geoid])
            # moved
            moved_rows = move_data_person[move_data_person[self.movecol] == self.moved_mark]
            if len(moved_rows) > 0:
                for row_id, row in moved_rows.iterrows():
                    if row[self.movecol] == self.moved_mark:
                        dt_str = "{}-{}-01".format(int(row[self.mvyear]), int(row[self.mvmonth]))
                        dt = pd.to_datetime(dt_str)
                        move_dates.append(dt)
                        move_geoids.append(row[self.geoid])
            move_info[hhidpn] = (move_dates, move_geoids)
        return move_info
    
    @staticmethod
    def find_index(dt, move_dt_list):
        if len(move_dt_list)==1:
            return 0
        for i, move_dt in enumerate(move_dt_list):
            if move_dt > dt:
                return i - 1
        return i
    
    def create_geoid_based_on_date(self, hhidpn: pd.Series, date: Union[pd.Timestamp, pd.Series]):
        # get all move timestamps
        if isinstance(date, pd.Timestamp):
            date = pd.Series([date for _ in range(len(hhidpn))])
        
        geoids = []
        assert len(hhidpn) == len(date)
        for i, idpn in enumerate(hhidpn):
            (move_dates, move_geoids) = self.move_info_dict[idpn]
            dt = date.iloc[i]
            geoid_idx = ResidentialHistoryHRS.find_index(dt, move_dates)
            geoids.append(move_geoids[geoid_idx])
        geoids = pd.Series(geoids).astype(str).str.zfill(11)
        assert len(geoids) == len(hhidpn)
        geoids.index = hhidpn.index
        return geoids


class HRSEpigenetics:
    def __init__(self, filename: str, datecol: str ='bcdate', move: bool=True, residential_hist: Union[None, ResidentialHistoryHRS]=None, hhidpn:str = "hhidpn"):
        self.df = pd.read_stata(filename)
        self.columns = self.df.columns
        assert datecol in self.columns, "Date column not in data!"
        self.datecol = datecol
        self.geoid_cols = [cname for cname in self.columns if "LINKCEN" in cname]
        self.move = move
        self.hhidpn = hhidpn
        if move:
            self.residential_hist = residential_hist
        else:
            self.format_geoid()

    def format_geoid(self) -> None:
        for geoid_col in self.geoid_cols:
            self.df[geoid_col] = self.df[geoid_col].astype(str).str.zfill(11)

    def get_geoid_based_on_date(self, datecol: pd.Series):
        geoid_col = self.residential_hist.create_geoid_based_on_date(self.df[self.hhidpn], datecol)
        return geoid_col
    
    def save(self, save_name) -> None:
        self.df.to_stata(save_name)

class LinkHRSHeat:
    @staticmethod
    def make_n_day_prior_cols(hrs_epi_data: HRSEpigenetics, n_day_prior: int) -> HRSEpigenetics:
        '''
        creates n-day prior column from the date column 
        '''
        colname = "{}_{}day_prior".format(hrs_epi_data.datecol, n_day_prior)
        nday_prior_col = hrs_epi_data.df[hrs_epi_data.datecol] - pd.Timedelta(n_day_prior, 'd')
        nday_prior_col.rename(colname, inplace=True)
        hrs_epi_data.df = pd.concat([hrs_epi_data.df, nday_prior_col], axis=1)
        return hrs_epi_data, colname

    @staticmethod
    def make_geoid_day_prior(hrs_epi_data: HRSEpigenetics, merge_date_colname: str, geoid_colname: str = "LINKCEN") -> HRSEpigenetics:
        '''
        creates geoid column to use for merging
        '''
        n_prior_str = '_'.join(merge_date_colname.split('_')[1:])
        colname = "{}_{}".format(geoid_colname, n_prior_str)
        if hrs_epi_data.move:
            n_day_prior_geoid = hrs_epi_data.get_geoid_based_on_date(hrs_epi_data.df[merge_date_colname])
            n_day_prior_geoid.rename(colname, inplace=True)
            hrs_epi_data.df = pd.concat([hrs_epi_data.df, n_day_prior_geoid], axis=1)
        else:
            hrs_epi_data.df[colname] = hrs_epi_data.df.apply(LinkHRSHeat.grab_geoid_for_year, axis=1, args=(merge_date_colname,))
        return hrs_epi_data, colname

    @staticmethod
    def grab_geoid_for_year(df_row, merge_date_colname) -> str:
        '''
        grabs the year from merge date column
        '''
        year = df_row[merge_date_colname].year
        geoid_col = "LINKCEN2010_{}".format(year)
        return df_row[geoid_col]

    @staticmethod
    def merge_with_heat_index(hrs_epi_data: HRSEpigenetics, heat_index_data_all: HeatIndexDataDir, left_on: list, drop_left: bool = True) -> HRSEpigenetics:
        # get all necessary years
        date_col = left_on[0]
        unique_years = hrs_epi_data.df[date_col].dt.year.unique()
        # nday prior col
        nday_prior_str = '_'.join(date_col.split('_')[1:])
        
        # process by year
        df_all_years = []
        for yr in unique_years:
            print("Linking year: {}".format(yr))
            heat_index_data = heat_index_data_all[yr]
            hrs_epi_df = hrs_epi_data.df[hrs_epi_data.df[date_col].dt.year == yr]
            right_on = [heat_index_data.date_col, heat_index_data.geoid_col]        
            # check overlapping column names
            heat_cols = [col for col in heat_index_data.columns if col not in right_on]
            for col in heat_cols:
                assert col not in hrs_epi_data.columns, "Heat data has column name {} that is overlapping with the HRS data".format(col)
            print("Merging...")
            hrs_epi_df = pd.merge(
                hrs_epi_df,
                heat_index_data.df,
                how='left',
                left_on=left_on,
                right_on=right_on,
                suffixes=(False, False) # throw error when both has colnames overlapping
            )
            # drop right keys
            hrs_epi_df.drop(right_on, axis=1, inplace=True)
            if drop_left:
                hrs_epi_df.drop(left_on[1:], axis=1, inplace=True)
            # rename data col
            hrs_epi_df = hrs_epi_df.rename(columns = {heat_index_data.data_col: "{}_{}".format(heat_index_data.data_col, nday_prior_str)})
            df_all_years.append(hrs_epi_df)

        hrs_epi_data.df = pd.concat(df_all_years)
        hrs_epi_data.df.sort_index(inplace=True)
        print("Done!")
        return hrs_epi_data
    