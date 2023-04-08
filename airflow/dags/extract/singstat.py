# tablebuilder.singstat.gov.sg/api/table/tabledata/{resourceId}

import json
import requests
import pandas as pd 

def get_cpi():
    urlData = "https://tablebuilder.singstat.gov.sg/api/table/tabledata/M212881"

    result = requests.get(urlData).json()
    result = result['Data']

    key_value_pairs = result['row'][0]['columns']
    # turn into a dataframe
    df = pd.DataFrame(key_value_pairs)
    # rename columns 
    df.columns = ['year_month', 'Value']
    # convert to datetime
    df['year_month'] = pd.to_datetime(df['year_month'], format='%Y %b')
    # rename columns
    df.columns = ['Month', 'Value']
    # set index
    # df.set_index('Month', inplace=True)

    # df.to_csv("../data/cpi.csv")
    return df