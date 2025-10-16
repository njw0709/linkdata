import os
from linkdata.hrs import HRSEpigenetics, LinkHRSHeat, ResidentialHistoryHRS
from linkdata.heat_index import HeatIndexDataDir
from tqdm import tqdm

hrs_data_dir = r'C:\Users\BioDem\Documents\BioDem\Users\Choi_ec\Heat Epigenetic Age\PrepData'
hrs_filename = 'HRSprep.dta' # prepared HRS file name
save_dir = r'C:\Users\BioDem\Documents\BioDem\Users\Choi_ec\Heat Epigenetic Age\Donotcopy'
save_filename = "HRSHeatMeanLinked.dta"

residential_hist_filename = r"C:\Users\BioDem\Documents\BioDem\Data\CDR Data\Residential History\4. reshist 1992_2018 moving month\_ALL1992_2018_reshit_long_mvdate.dta"

heat_data_dir = r'C:\Users\BioDem\Documents\BioDem\Users\Nam\Linkage\data\daily_heat_mean_long'
heat_data_all = HeatIndexDataDir(heat_data_dir)

residential_hist = ResidentialHistoryHRS(residential_hist_filename)
hrs_epi_data = HRSEpigenetics(os.path.join(hrs_data_dir, hrs_filename), datecol="bcdate", move=True, residential_hist=residential_hist)

for n in tqdm(range(2191)):
    hrs_epi_data, n_day_colname = LinkHRSHeat.make_n_day_prior_cols(hrs_epi_data, n)
    hrs_epi_data, n_day_geoid_colname = LinkHRSHeat.make_geoid_day_prior(hrs_epi_data, n_day_colname)
    hrs_epi_data = LinkHRSHeat.merge_with_heat_index(hrs_epi_data, heat_data_all, left_on=[n_day_colname, n_day_geoid_colname], drop_left=False)
    if n%10 ==0:
        # save data 
        print("Saving...")
        hrs_epi_data.save(os.path.join(save_dir, save_filename))

# save data 
print("Saving Final...")
hrs_epi_data.save(os.path.join(save_dir, save_filename))