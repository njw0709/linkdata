from .heat_index import HeatIndexData, HeatIndexDataDir
import os
from typing import Union

FILENAME_TO_VARNAME_DICT = {
    "tmmx": "Tmax",
    "rmin": "Rmin",
    "pm25": "pm25",
    "ozone": "o3",
}


class DailyMeasureData(HeatIndexData):
    def __init__(
        self,
        file_name: str,
        heat_index_col: str,
        read_dtype: str = "float32",
        format: str = "long",
        geoid_col: str = "GEOID10",
        date_col: str = "Date",
        rename_col: Union[dict, None] = None,
    ):
        super().__init__(
            file_name,
            read_dtype=read_dtype,
            format=format,
            geoid_col=geoid_col,
            date_col=date_col,
            heat_index_col=heat_index_col,
            rename_col=rename_col,
        )


class DailyMeasureDataDir(HeatIndexDataDir):
    def __init__(
        self,
        dir_name: str,
        measure_type: str,
        rename_col_dict: Union[dict, None] = None,
        column_name_to_choose: Union[None, str] = None,
    ):
        super().__init__(dir_name)
        self.files = [
            f for f in os.listdir(dir_name) if f.endswith("csv") and measure_type in f
        ]
        self.years_available = self.get_all_years_available()
        self.data = {yr: None for yr in self.years_available}
        self.rename_col_dict = rename_col_dict
        self.measure_type = measure_type
        self.column_to_choose = column_name_to_choose

    def __getitem__(self, year: int) -> DailyMeasureData:
        year_key = str(year)
        if self.data[year_key] is None:
            filename = self.files[self.years_available.index(year_key)]
            print("loading file:{}".format(filename))
            if year_key in self.rename_col_dict.keys():
                rename_col = self.rename_col_dict[year_key]
            else:
                if self.rename_col_dict is None:
                    rename_col = None
                else:
                    rename_col = self.rename_col_dict.copy()
            if self.column_to_choose is None:
                colname = FILENAME_TO_VARNAME_DICT[self.measure_type]
            else:
                colname = self.column_to_choose
            self.data[year_key] = DailyMeasureData(
                os.path.join(self.dirname, filename), colname, rename_col=rename_col
            )
        return self.data[year_key]
