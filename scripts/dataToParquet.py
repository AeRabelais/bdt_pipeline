#create and update the parquet files for air and soil, separately
import os 
import pandas as pd
import glob

#file directories for the air and soil files
#air_dir = r"D:\sample_biodiversitree\data\export_data\air_data"
soil_dir = r"D:\sample_biodiversitree\scripts\data\export_data\soil_data"



#all_air_files = glob.glob(air_dir + '/**/*.csv', recursive=True)

all_soil_files = glob.glob(soil_dir + '/**/*.csv', recursive=True)


#air_data = pd.concat((pd.read_csv(f) for f in all_air_files ))
#air_data.to_parquet('air_data.parquet')

#need to look at soil's clean up job

soil_data = pd.concat((pd.read_csv(f) for f in all_soil_files ))
soil_data.to_parquet('soil_data.parquet')








