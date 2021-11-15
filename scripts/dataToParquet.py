"""
@Title: dataToParquet.py
@author: Ashia Lewis

GOAL: Create and update the parquet files for the air and soil data, separately.
"""
import os
import glob
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

#CODE TO BE USED FOR THE BATCH DATA
"""
#file directories for the air and soil files
air_dir = r"D:\sample_biodiversitree\data\export_data\air_data"
soil_dir = r"D:\sample_biodiversitree\scripts\data\export_data\soil_data"



#all_air_files = glob.glob(air_dir + '/**/*.csv', recursive=True)

all_soil_files = glob.glob(soil_dir + '/**/*.csv', recursive=True)


#air_data = pd.concat((pd.read_csv(f) for f in all_air_files ))
#air_data.to_parquet('air_data.parquet')

#need to look at soil's clean up job

soil_data = pd.concat((pd.read_csv(f) for f in all_soil_files ))
soil_data.to_parquet('soil_data.parquet')

"""

#CODE TO BE USED IN THE ACTUAL PIPELINE

#   file directories for the air and soil files 
air_dir = r"D:\sample_biodiversitree\data\export_data\air_data"
soil_dir = r"D:\sample_biodiversitree\data\export_data\soil_data"

#concatentate all of files' data
all_air_files = glob.glob(air_dir + '/**/*.csv', recursive=True)
all_soil_files = glob.glob(soil_dir + '/**/*.csv', recursive=True)

#put the data in a dataframe 
air_data = pd.concat((pd.read_csv(f) for f in all_air_files))
soil_data = pd.concat((pd.read_csv(f) for f in all_soil_files))

#add data to existing parquet files
air_table = pa.Table.from_pandas(air_data)
soil_table = pa.Table.from_pandas(soil_data)


air_writer = pq.ParquetWriter('air_data.parquet', air_table.schema)
air_writer.write_table(table = air_table)

if air_writer:
    air_writer.close()

soil_writer = pq.ParquetWriter('soil_data.parquet', soil_table.schema)
soil_writer.write_table(table = soil_table)

if soil_writer:
    soil_writer.close()




