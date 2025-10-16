import pandas as pd
from typing import List, Union
import os

class HeatIndexData:
    def __init__(self, 
                 file_name: str, 
                 read_dtype: str ='float32', 
                 format: str ='long', 
                 geoid_col: str = "GEOID10", 
                 date_col: str = "Date", 
                 heat_index_col: str = 'HeatIndex',
                 rename_col: Union[dict, None] = None):
        self.filename = file_name
        self.date_col = date_col
        self.geoid_col = geoid_col
        self.data_col = heat_index_col      
        if read_dtype != 'float64':
            if rename_col is not None:
                df = pd.read_csv(file_name, nrows=1)
                df = df.rename(columns=rename_col)
            data_cols = [heat_index_col]
            dtype_dict = {col: read_dtype for col in data_cols} # assume first column is index
        else:
            dtype_dict = {}

        if rename_col is not None:
            df = pd.read_csv(file_name, nrows=1)
            self.df = df.rename(columns=rename_col)
            self.df.astype(dtype_dict)
        else:
            self.df = pd.read_csv(file_name, dtype=dtype_dict, nrows=1)
        self.columns = self.df.columns
        self.format = self.check_long_or_wide()
        if self.format == 'wide' and format=='long':
            self.df = pd.read_csv(file_name)
            self.convert_to_long(value_name=self.data_col)
            self.format = 'long'
        else:
            if rename_col is not None:
                df = pd.read_csv(file_name)
                self.df = df.rename(columns=rename_col)
                self.df.astype(dtype_dict)
                self.df = self.df[[date_col, geoid_col, heat_index_col]]
            else:
                self.df = pd.read_csv(file_name, dtype=dtype_dict, usecols=[date_col, geoid_col, heat_index_col])
        self.format_cols()

    def check_long_or_wide(self):
        if len(self.columns) > 4:
            return "wide"
        else:
            return "long"
    
    def convert_to_long(self, date_col_index: int = 0, var_name:str = "GEOID10", value_name:str = "HeatIndex"):
        # converts dataframe with date on index and geoid on the columns into long format
        df = self.df.rename(columns={self.columns[date_col_index]: "Date"})
        df = df.melt(id_vars=["Date"], var_name=var_name, value_name=value_name)
        self.df = df

    def __repr__(self):
        return str(self.df.head(5))

    def head(self, n:int = 5):
        print(self.df.head(n))

    def format_cols(self) -> None:
        # change date col as datetime
        self.df[self.date_col] = self.df[self.date_col].astype('datetime64[ns]')
        # format geoid
        self.df[self.geoid_col] = self.df[self.geoid_col].astype(str).str.zfill(11)

class HeatIndexDataDir:
    def __init__(self, dir_name: str):
        self.dirname = dir_name
        self.files = [f for f in os.listdir(dir_name) if f.endswith('csv')]
        self.years_available = self.get_all_years_available()
        self.data = {yr: None for yr in self.years_available}

    def get_all_years_available(self) -> List:
        years_available = [f.split('_')[0] for f in self.files]
        return years_available
    
    def __getitem__(self, year: int) -> HeatIndexData:
        year_key = str(year)
        if self.data[year_key] is None:
            filename = self.files[self.years_available.index(year_key)]
            print("loading file:{}".format(filename))
            self.data[year_key] = HeatIndexData(os.path.join(self.dirname, filename))
        return self.data[year_key]
    
