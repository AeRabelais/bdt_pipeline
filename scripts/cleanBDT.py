"""
@Title: clean_bdt.py
@author: Ashia Lewis

GOAL: Clean the BiodiversiTree export data, and the save errors to a csv.
"""
import os
import pandas as pd
import numpy as np

#CODE FOR THE BATCH DATA

export_data = r"D:\sample_biodiversitree\data\export_data"

def findOutliers(df):
    q1 = df.quantile(0.25)
    q3 = df.quantile(0.75)
    iqr = q3 - q1

    outlier_values = bdt_df[(bdt_df < (q1 - 1.5 * iqr)) | (bdt_df > (q3 + 1.5 * iqr)).any(axis=0)]
    
    return outlier_values

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
            # possible: invalid_time_rows = bdt_df[bdt_df['time2'] < '2018-01-01 00:00:00'].index
            invalid_time_rows = bdt_df[bdt_df['time2'].str.contains(os.path.basename(file_path)[-3:], case=False) is False].index
            if(invalid_time_rows.size >= 1):
                bdt_df.drop(labels = invalid_time_rows, axis=0, inplace=True)
            #replace NA values with null
            nan_val_rows = bdt_df[(bdt_df == 'NA').any(axis=1)]
            bdt_df.replace('NA', np.nan)
            #fix the div values that come in as text, rather than as integers
            bdt_df['div'] = bdt_df['div'].replace('nr', 0)
            bdt_df['div'] = bdt_df['div'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
            #remove any serious outlier values
            outlier_values = findOutliers(bdt_df)
            bdt_df = bdt_df.drop(labels = outlier_values, axis = 0)

            #update the cleaned csv file
            bdt_df.to_csv(file_path, index= False, header = True)

            #save each of the error types as separate csv files to pull from
            invalid_time_rows.to_csv(os.path.join(year_path, "invalid_times.csv"), index= False, header = True, mode='a')
            nan_val_rows.to_csv(os.path.join(year_path, "nan_values.csv"), index= False, header = True, mode='a')
            outlier_values.to_csv(os.path.join(year_path, "outlier_values.csv"), index= False, header = True, mode='a')




#CODE FOR THE AUTOMATION PIPELINE
"""

import os
import pandas as pd
import numpy as np

export_data = r"D:\sample_biodiversitree\scripts\data\export_data\export_new"



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

"""

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



    

