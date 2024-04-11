## importing the packages
import pandas as pd
import os
from pathlib import Path
import duckdb
import numpy as np
import sys

current_working_directory = os.getcwd()
database_directory = os.path.join(
            current_working_directory, "pipeline/data", "database"
        )

source_directory = os.path.join(
            current_working_directory, "pipeline/data", "source"
        )

def extract_data(dataset_path):
    """
    Extract the covid datasets from the csv file into dataframe and return the dataframe

    Parameters
    ----------
    dataset_path : str
        Name of the path and the file name 
    """
    try:
        covid = pd.read_csv(dataset_path)
    except FileNotFoundError:
        print("File not found.")
    except pd.errors.EmptyDataError:
        print("No data")
    except pd.errors.ParserError:
        print("Parse error")
    return covid


def transform(df_list, statename):
    """
    Transform the dataframes by concatinating all the given dataframes
    Filter the dataframe for the given statename
    Create new columns - DailyCases and DailyDeaths
    Return the transformed dataframe

    Parameters
    ----------
    df_list : list
        list of dataframe names
    statename : str
        Name of the state to be used to filter the records
    """
    ### Concating the dataframes into one single dataframe
    covid = pd.concat(df_list)

    ### Filtering the dataframe for the given state
    covid = covid[covid.state == statename]

    ### Adding Daily cases and Daily Deaths
    covid = covid[covid.state == statename].sort_values(by=['county', 'date'])
    covid['DailyCases'] = covid['cases'].diff().fillna(0).astype('Int64')
    covid['DailyDeaths'] = covid['deaths'].diff().fillna(0).astype('Int64')

    return covid


def load(data, name, duckdb_con):
    """
    Load the dataset in the csv format to the path given along with the file name

    Parameters
    ----------
    data : dataframe
        name of the transformed dataframe
    name : str
        Name of the state along with the path where to write the csv file
    """
    name = name.replace(" ", "")
    duckdb_con.execute(f"DROP TABLE IF EXISTS {name}")
    duckdb_con.from_df(data).create(name)


def generate_covidreport_statewise(statename):
    """
    Generate the final report for the given state

    Parameters
    ----------
    statename : string
        name of the state 
    """
    try:
        print(f"Generating the report for the state ::: {statename}")
        ##extracting the data
        covid2020 = extract_data(os.path.join(source_directory, "us-counties-2020.csv"))
        covid2021 = extract_data(os.path.join(source_directory, "us-counties-2021.csv"))
        covid2022 = extract_data(os.path.join(source_directory, "us-counties-2022.csv"))
        covid2023 = extract_data(os.path.join(source_directory, "us-counties-2023.csv"))
        print("Extraction is completed")

        ##transforming the data
        covid = transform([covid2020, covid2021, covid2022, covid2023], statename)
        
        ##loading the dataset
        # Creating a new directory for DuckDB tables
          
        Path(database_directory).mkdir(parents=True, exist_ok=True)

        # Creating DuckDB file at new directory
        duckdb_file_path = os.path.join(database_directory, "covid.duckdb")
        duckdb_con = duckdb.connect(duckdb_file_path)
        load(covid, statename, duckdb_con)
        print(f"Report generation completed")
    except Exception as e:
        print(e, file=sys.stderr)
    
    
if __name__ == "__main__":
    state_names = ["Alaska", "Alabama", "Arkansas", "American Samoa", "Arizona", "California", "Colorado", "Connecticut", "District ", "of Columbia", "Delaware", "Florida", "Georgia", "Guam", "Hawaii", "Iowa", "Idaho", "Illinois", "Indiana", "Kansas", "Kentucky", "Louisiana", "Massachusetts", "Maryland", "Maine", "Michigan", "Minnesota", "Missouri", "Mississippi", "Montana", "North Carolina", "North Dakota", "Nebraska", "New Hampshire", "New Jersey", "New Mexico", "Nevada", "New York", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Virginia", "Virgin Islands", "Vermont", "Washington", "Wisconsin", "West Virginia", "Wyoming"]
    for state in state_names:
        generate_covidreport_statewise(state)