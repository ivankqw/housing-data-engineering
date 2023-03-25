import pandas as pd
import requests

# read districts excel 
# https://www.ura.gov.sg/realEstateIIWeb/resources/misc/list_of_postal_districts.htm
data_path = "/opt/airflow/dags/data/"

def read_and_transform_districts():

    districts = pd.read_excel(data_path + 'districts.xlsx')

    # read districts df and convert Postal Sector column which is a string of items separated by comma, to a list
    districts['Postal Sector'] = districts['Postal Sector'].str.split(',')
    # same for General Loation
    districts['General Location'] = districts['General Location'].str.split(',')

    # Columns Postal Sector and General Location apply explode function to convert each item in the list to a row
    districts = districts.explode('Postal Sector')
    districts = districts.explode('General Location')

    # remove whitespace in postal sector
    districts['Postal Sector'] = districts['Postal Sector'].str.strip()

    return districts

def transform_resale_flats(filename, df_districts):

    districts = df_districts

    resale_flats = pd.read_csv(filename)

    # split month column which is in the format of YYYY-MM into 2 columns
    resale_flats['month'] = resale_flats['month'].str.split('-')

    resale_flats['year'] = resale_flats['month'].apply(lambda x: int(x[0]))
    resale_flats['month'] = resale_flats['month'].apply(lambda x: int(x[1]))

    # take only most recent 5 years of data
    # sort indescending order of year then month

    resale_flats = resale_flats[resale_flats['year'] >= 2017]
    resale_flats = resale_flats.sort_values(by=['year', 'month'], ascending=False)
    print(resale_flats.shape)

    # take first 1000 for testing purposes
    # resale_flats = resale_flats.head(1000)

    # use openmap api to get postal code and district from the street name
    # add dynamic programming to cache the results if same address is called again
    dict1 = {}

    def get_info_from_street_name(address):
        # if address is in dict1, return the value
        if address in dict1:
            return dict1[address]
        # else call the api and add to dict1
        else:
            
            url = "https://developers.onemap.sg/commonapi/search?searchVal={}&returnGeom=Y&getAddrDetails=Y".format(address)
            response = requests.get(url)
            result = response.json()['results'][0]
            postal = result['POSTAL']
            x = result['X']
            y = result['Y']
            lat = result['LATITUDE']
            lon = result['LONGITUDE']
            dict1[address] = (postal, x, y, lat, lon)
            
            return (postal, x, y, lat, lon)

    # %%
    # Apply get_info_from_street_name function to each row using a new column created by concat block and street name
    resale_flats['street_name_with_block'] = resale_flats['block'] + ' ' + resale_flats['street_name']
    resale_flats['postal'], resale_flats['x'], resale_flats['y'], resale_flats['lat'], resale_flats['lon'] = zip(*resale_flats['street_name_with_block'].apply(get_info_from_street_name))

    # function to get district from postal code from districts table
    def get_district_from_postal(postal):
        postal_sector = str(postal)[:2]
        value = districts[districts['Postal Sector'] == postal_sector]['Postal District'].values
        if not len(value):
            print(postal)
            return 'NIL'
        return value[0]

    # apply get_district_from_postal function to each row using a new column created by postal code
    resale_flats['district'] = resale_flats['postal'].apply(get_district_from_postal)

    # remove rows with district as NIL
    resale_flats = resale_flats[resale_flats['district'] != 'NIL']

    return resale_flats

def transform_private_transactions_and_rental(filename_private_transactions, filename_private_rental, df_districts):
    # read the private_transactions csv file
    private_transactions = pd.read_csv(filename_private_transactions)
    # and private_rental
    private_rental = pd.read_csv(filename_private_rental)

    private_transactions.head()

    # %%
    def private_get_month_year(date):
        date = str(date)
        # date is in format of MYY or MMYY
        # if date is in format of MYY, add 0 in front
        if len(date) == 3:
            date = '0' + date
        month = date[:2]
        year = date[2:]
        # add prefix to year 20
        year = '20' + year
        return (month, year)

    # %%
    # apply private_get_month_year function to each row, creating 2 new columns for month and year
    private_transactions['month'], private_transactions['year'] = zip(*private_transactions['contractDate'].apply(private_get_month_year))
    # same for private_rental on leaseDate
    private_rental['month'], private_rental['year'] = zip(*private_rental['leaseDate'].apply(private_get_month_year))

    return private_transactions, private_rental
