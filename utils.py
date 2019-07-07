"""
This module is used for processing with .csv
"""
import os
import pandas as pd

"""
read_csv by pd
Read a comma-separated values (csv) file into DataFrame.
"""
def read_csv_file(csvpath):
    #read csv, return df
    return pd.read_csv(csvpath)

"""
list all csv files in dirpath, and return them as a file list
"""
def get_csv_from_dir(dirpath):
    # list all csvs
    csv_files = os.listdir(dirpath)
    # csv file list filter
    csv_files_list = [os.path.join(dirpath, csv) for csv in csv_files if csv.endswith(".csv")]
    return csv_files_list