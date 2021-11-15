#The data file that will be used to clean the data in the automation 
#pipeline

'''
import os
import pandas as pd
import numpy as np

export_data = r"D:\sample_biodiversitree\data\export_data"
data_errors = []

for element_folder in os.listdir(export_data):
    element_path = os.path.join(export_data, element_folder)
    for year_folder in os.listdir(element_path):
        year_path = os.path.join(element_path, year_folder)
        for file_csv in os.listdir(year_path):
            file_path = os.path.join(year_path, file_csv)
            #read in the csv file
            dateCols = ['time2']
            bdt_df = pd.read_csv(file_path, parse_dates=dateCols, low_memory=True)
            #ensure that time stamps are within reason
            invalid_time_rows = bdt_df[bdt_df['time2'] < '2017-01-01 00:00:00'].index
            if(len(invalid_time_rows) > 1):
                data_errors.append(['invalid_time_rows', invalid_time_rows])
                bdt_df.drop(labels = invalid_time_rows, axis=0, inplace=True)
            #replace NA values with null
            nan_val_rows = bdt_df[(bdt_df == 'NA').any(axis=1)].index
            data_errors.append(['null_vals', nan_val_rows])
            bdt_df.replace('NA', np.nan)
            bdt_df['div'] = bdt_df['div'].replace('nr', 0)
            #replace the outlier values with null
            bdt_df.to_csv(file_path, index= False, header = True)

#add the bit about outputing the data errors
    # out -> csv file with the data errors info
'''

import os
import pandas as pd
import numpy as np

export_data = r"D:\sample_biodiversitree\scripts\data\export_data\export_new"
#remember after cleaning to (1) move new files to the proper folder, (2) delete them 



def findOutliers(bdt_df):
    q1 = bdt_df.quantile(0.25)
    q3 = bdt_df.quantile(0.75)
    iqr = q3 - q1

    outlier_values = bdt_df[(bdt_df < (q1 - 1.5 * iqr)) | (bdt_df > (q3 + 1.5 * iqr)).any(axis=0)].index
    
    return outlier_values

def cleanExportData(bdt_df):
    data_errors = pd.DataFrame
    for file_csv in os.listdir(export_data):
        file_path = os.path.join(export_data, file_csv)
        dateCols = ['time2']
        bdt_df = pd.read_csv(file_path, parse_dates=dateCols, low_memory=True)
        #ensure that time stamps are within reason
        # possible: invalid_time_rows = bdt_df[bdt_df['time2'] < '2018-01-01 00:00:00'].index
        invalid_time_rows = bdt_df[bdt_df['time2'].str.contains(os.path.basename(file_path)[-3:], case=False) is False]
        if(len(invalid_time_rows) > 1):
            bdt_df.drop(labels = invalid_time_rows, axis=0, inplace=True)
        #replace NA values with null
        nan_val_rows = bdt_df[(bdt_df == 'NA').any(axis=1)].index
        bdt_df.replace('NA', np.nan)
        bdt_df['div'] = bdt_df['div'].replace('nr', 0)
        bdt_df['div'] = bdt_df['div'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
        #remove any serious outlier values
        outlier_values = findOutliers(bdt_df)
        bdt_df = bdt_df.drop(labels = outlier_values, axis = 0)

        #generate the error rows as a csv
        return data_errors
    

# TO-DO:
# (1) generate the flagging report

'''
New Directory Structure:


//data
    -> export_data
        -> air_data
            -> air_18
                -> individual csv file. Example: "BDT_air_1_2018(.csv)"
            -> ....
        -> soil_data
            -> soil_18
                -> individual csv file. Example: "BDT_soil_1_2018(.csv)"
            -> ....
        -> export_new
    -> normalized_data
        -> norm_new 
        -> normalized_18 
        -> ....
    -> processed_data
        -> proc_new
        -> proc_storage_18
        -> ....
    -> raw_data
        -> raw_dir_18 
        -> ....
        -> raw_dir_temp
        -> raw_total_storage
        -> raw_new


STEPS:

- 1 - 
'''



    
            
            





            



