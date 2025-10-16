import os
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

heat_index_dir = r"C:\Users\BioDem\Documents\BioDem\Users\Nam\Linkage\data\daily_heat_mean"
heat_data_list = [f for f in os.listdir(heat_index_dir) if f.endswith('csv')]
heat_long_save_dir = r"C:\Users\BioDem\Documents\BioDem\Users\Nam\Linkage\data\daily_heat_mean_long"

def convert_to_long(df:pd.DataFrame, date_col_index: int = 0, var_name:str = "GEOID10", value_name:str = "HeatIndex"):
    # converts dataframe with date on index and geoid on the columns into long format
    df = df.rename(columns={df.columns[date_col_index]: "Date"})
    df = df.melt(id_vars=["Date"], var_name=var_name, value_name=value_name)
    return df

def convert_long_and_save(heat_file_name):
    heat_df = pd.read_csv(os.path.join(heat_index_dir, heat_file_name))
    heat_long = convert_to_long(heat_df)
    heat_long.to_csv(os.path.join(heat_long_save_dir, heat_file_name))


for heat_file in tqdm(heat_data_list):
    convert_long_and_save(heat_file)

# with ThreadPoolExecutor(max_workers=1) as executor:
#     futures = {executor.submit(convert_long_and_save, f): f for f in heat_data_list}

#     with tqdm(total=len(heat_data_list)) as progress:
#         for future in as_completed(futures):
#             result = future.result()
#             progress.update(1)