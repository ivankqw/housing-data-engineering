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

    # drop the general location column
    districts = districts.drop(columns=['General Location'])

    # remove duplicate rows
    districts = districts.drop_duplicates()

    # remove rows where postal sector is not 2 digit
    districts = districts[districts['Postal Sector'].str.len() == 2]

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

    # read addresses.csv, if file exists and populate dict1
    try:
        addresses = pd.read_csv(data_path + 'addresses.csv')
        print(addresses.shape)
        print("Addresses file exists")
        for index, row in addresses.iterrows():
            dict1[row['address']] = (row['postal'], row['x'], row['y'], row['lat'], row['lon'])
        
    except:
        pass

    def get_info_from_street_name(address):
        # if address is in dict1, return the value
        if address in dict1:
            # print("Address in dict1", address, dict1[address])
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

    # save dict1 to a csv file, with the first column named 'address' and the rest named 'postal', 'x', 'y', 'lat', 'lon'
    df = pd.DataFrame.from_dict(dict1, orient='index', columns=['postal', 'x', 'y', 'lat', 'lon'])
    df.index.name = 'address'
    df.to_csv(data_path + 'addresses.csv')

    # remove rows with district as NIL
    resale_flats = resale_flats[resale_flats['district'] != 'NIL']

    return resale_flats

def transform_private_transactions_and_rental(filename_private_transactions, filename_private_rental):
    # read the private_transactions csv file
    private_transactions = pd.read_csv(filename_private_transactions)
    # and private_rental
    private_rental = pd.read_csv(filename_private_rental)

    # add a column _id for both private_transactions and private_rental, integer, auto increment
    private_transactions['_id'] = range(1, len(private_transactions) + 1)
    private_rental['_id'] = range(1, len(private_rental) + 1)

    # set index _id
    private_transactions = private_transactions.set_index('_id')
    private_rental = private_rental.set_index('_id')

    
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

    
    # apply private_get_month_year function to each row, creating 2 new columns for month and year
    private_transactions['month'], private_transactions['year'] = zip(*private_transactions['contractDate'].apply(private_get_month_year))
    # same for private_rental on leaseDate
    private_rental['month'], private_rental['year'] = zip(*private_rental['leaseDate'].apply(private_get_month_year))

    return private_transactions, private_rental

def transform_salesperson_transactions(filename):
    # read the salesperson_transactions csv file
    salesperson_transactions = pd.read_csv(filename)

    # for district column, replace "-" with NULL for sql
    salesperson_transactions['district'] = salesperson_transactions['district'].replace('-', 'NULL')

    # for transaction_date column in the format of mmm-yy (e.g. Jan-19), create new 2 columns, one for month and one for year
    salesperson_transactions['transaction_date'] = salesperson_transactions['transaction_date'].str.split('-')
    salesperson_transactions['month'] = salesperson_transactions['transaction_date'].apply(lambda x: x[0])
    # for the year, add prefix 20
    salesperson_transactions['year'] = salesperson_transactions['transaction_date'].apply(lambda x: '20' + x[1])

    # drop transaction_date column
    salesperson_transactions = salesperson_transactions.drop(columns=['transaction_date'])

    return salesperson_transactions