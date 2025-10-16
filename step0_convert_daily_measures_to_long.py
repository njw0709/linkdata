import os
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

heat_index_dir = r"C:\Users\BioDem\Documents\BioDem\Users\Choi_ec\Linkage\data\daily_measures"
measures = ['tmmx', 'rmin']
value_names = {'tmmx': 'Tmax', 'rmin': 'Rmin'}
measure_data_list = [f for f in os.listdir(heat_index_dir) if f.endswith('csv')]
interested_measure_data_list = []
for f in measure_data_list:
    if any([m in f for m in measures]):
        interested_measure_data_list.append(f)

print(interested_measure_data_list)


daily_long_save_dir = r"C:\Users\BioDem\Documents\BioDem\Users\Choi_ec\Linkage\data\daily_measures_long"


def convert_to_long(df:pd.DataFrame, date_col_index: int = 0, var_name:str = "GEOID10", value_name:str = "HeatIndex"):
    # converts dataframe with date on index and geoid on the columns into long format
    df = df.rename(columns={df.columns[date_col_index]: "Date"})
    df = df.melt(id_vars=["Date"], var_name=var_name, value_name=value_name)
    return df

def convert_long_and_save(heat_file_name):
    if 'tmmx' in heat_file_name:
        value_name = value_names['tmmx']
    else:
        value_name = value_names['rmin']
    heat_df = pd.read_csv(os.path.join(heat_index_dir, heat_file_name))
    heat_long = convert_to_long(heat_df, value_name=value_name)
    heat_long.to_csv(os.path.join(daily_long_save_dir, heat_file_name))



with ThreadPoolExecutor(max_workers=24) as executor:
    futures = {executor.submit(convert_long_and_save, f): f for f in interested_measure_data_list}

    with tqdm(total=len(interested_measure_data_list)) as progress:
        for future in as_completed(futures):
            result = future.result()
            progress.update(1)