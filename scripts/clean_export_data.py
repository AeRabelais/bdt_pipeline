"""
@Title: clean_bdt.py
@author: Ashia Lewis

GOAL: Clean the BiodiversiTree export data, and the save errors to a csv.
"""


#imports
import os
import pandas as pd
import numpy as np
import glob


#ensure timestamps are within reason
def validate_timestamps(df):

    lower_qrt = df['time2'].quantile(0.1) #lower 10%
    upper_qrt = df['time2'].quantile(0.9) #higher 10%

    #drop dates that are not within the threshold
    invalid_time_rows = df[(df['time2'] < lower_qrt) & (df['time2'] > upper_qrt)].copy()
    invalid_time_rows['error_type'] = 'invalid_time_rows'

    df = df[(df['time2'] >= lower_qrt) & (df['time2'] <= upper_qrt)]

    #drop duplicated values
    df.drop_duplicates(subset=['time2'])

    return df, invalid_time_rows

#replace NA values with null values
def fix_nulls(df):

    df.replace('NA', np.nan)
    nan_val_rows = df[(df.isna().any(axis=1))]
    nan_val_rows['error_type'] = 'nan_val_rows'
    df.replace('NA', np.nan)

    return df, nan_val_rows

#fix the div values that come in as text, rather than as integers
def correct_labels(df):

    df['div'] = df['div'].replace(['forest', 'field','nr'], [13, 14, 15])

    return df

#define the outlier values
def drop_outliers(df, cols):

    outlier_values = df[(df[cols] >= 75)]
    outlier_values['error_type'] = 'outlier values'

    df = df[(df[cols] < 75)]

    return df, outlier_values


#collate each of the files by element
all_files = glob.glob(r"D:\sample_biodiversitree\data\export_data\**\*new_copy\*.csv", recursive=True) #replace with [glob.glob(r"D:\sample_biodiversitree\data\export_data\**\**\*.csv", recursive=True)] for entire batch

# -- for the automation pipeline
#air_files = glob.glob(r"D:\sample_biodiversitree\data\export_data\air_data\**\*.csv", recursive=True)
#soil_files = glob.glob(r"D:\sample_biodiversitree\data\export_data\soil_data\**\*.csv", recursive=True)

#iterate through the cleaning system
for file_path in all_files:
    dateCols = ['time2']
    bdt_df = pd.read_csv(file_path, parse_dates=dateCols, low_memory=True)
    #air_df = pd.concat(map(pd.read_csv, air_files), ignore_index=True) --change this for when the data is of the automation pipeline
    #soil_df = pd.concat(map(pd.read_csv, soil_files), ignore_index=True) --change this for when the data is for the automation pipeline
    if 'airtemp' in bdt_df.columns:
        cols = 'airtemp'
        error_path = os.path.join(r"D:\sample_biodiversitree\data\error_reports", "error_report_air.csv")
    else:
        cols = 'temp'
        error_path = os.path.join(r"D:\sample_biodiversitree\data\error_reports", "error_report_soil.csv")
    #fix the diversity values
    bdt_df = correct_labels(bdt_df)
    #fix null values
    bdt_df, nan_val_rows = fix_nulls(bdt_df)
    #validate the right timestamps
    bdt_df, invalid_time_rows = validate_timestamps(bdt_df)
    #drop the outlier values and store errors
    bdt_df, outlier_vals = drop_outliers(bdt_df, cols)

    #put the cleaned export files in their respective folder
    bdt_df.to_csv(file_path, index= False, header = True)

    #put the error dataframes in their respective folders to be used later
    #add a function to 'uniqify' the error report names later on
    errors = pd.concat([outlier_vals, nan_val_rows, invalid_time_rows], ignore_index=True) #--add the outlier values
    
    if os.path.exists(error_path):
        errors.to_csv(error_path,index= False, header = False, mode='a')
    else:
        errors.to_csv(error_path,index= False, header = True)
