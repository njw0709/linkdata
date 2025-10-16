import os
from linkdata.hrs import HRSEpigenetics, LinkHRSHeat, ResidentialHistoryHRS
from linkdata.daily_measure import DailyMeasureDataDir
from tqdm import tqdm

hrs_data_dir = r'C:\Users\BioDem\Documents\BioDem\Users\Choi_ec\Heat Epigenetic Age\PrepData'
hrs_filename = 'HRSprep.dta' # prepared HRS file name
save_dir = r'C:\Users\BioDem\Documents\BioDem\Users\Choi_ec\Heat Epigenetic Age\Donotcopy'
save_filename = "HRSPM25O3Linked.dta"

residential_hist_filename = r"C:\Users\BioDem\Documents\BioDem\Data\CDR Data\Residential History\4. reshist 1992_2018 moving month\_ALL1992_2018_reshit_long_mvdate.dta"

pm25_dir = r'C:\Users\BioDem\Documents\BioDem\Users\Nam\Linkage\data\PM25'
o3_dir = r'C:\Users\BioDem\Documents\BioDem\Users\Nam\Linkage\data\O3'

pm25_rename_cols = {
    "date": "Date", 
    "fips": "GEOID10", 
    "pm25_daily_averageugm3": "pm25"
}
o3_rename_cols = {
    "Date": "Date",
    "Loc_Label1": "GEOID10",
    "Prediction": "o3"
}

pm25_data_all = DailyMeasureDataDir(pm25_dir, 'pm25', rename_col_dict=pm25_rename_cols)
o3_data_all = DailyMeasureDataDir(o3_dir, 'ozone', rename_col_dict=o3_rename_cols)

residential_hist = ResidentialHistoryHRS(residential_hist_filename)
hrs_epi_data = HRSEpigenetics(os.path.join(hrs_data_dir, hrs_filename), datecol="bcdate", move=True, residential_hist=residential_hist)

for n in tqdm(range(366)):
    hrs_epi_data, n_day_colname = LinkHRSHeat.make_n_day_prior_cols(hrs_epi_data, n)
    hrs_epi_data, n_day_geoid_colname = LinkHRSHeat.make_geoid_day_prior(hrs_epi_data, n_day_colname)
    hrs_epi_data = LinkHRSHeat.merge_with_heat_index(hrs_epi_data, pm25_data_all, left_on=[n_day_colname, n_day_geoid_colname], drop_left=False)
    hrs_epi_data = LinkHRSHeat.merge_with_heat_index(hrs_epi_data, o3_data_all, left_on=[n_day_colname, n_day_geoid_colname])
    if n%10 ==0:
        # save data 
        print("Saving...")
        hrs_epi_data.save(os.path.join(save_dir, save_filename))

# save data 
print("Saving Final...")
hrs_epi_data.save(os.path.join(save_dir, save_filename))