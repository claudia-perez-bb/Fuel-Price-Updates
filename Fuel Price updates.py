import json
import requests
import pandas as pd
from datetime import datetime,timedelta
import pyodbc
from sqlalchemy import create_engine
import sqlalchemy.types
from bs4 import BeautifulSoup
import urllib
import os
from sqlalchemy import text

# US

# Find date 2 months ago so it can go into the api
two_months_ago = datetime.now() - timedelta(days=60)

# Format the as string date format
start_date = two_months_ago.strftime('%Y-%m-%d')

# Grab the api from the EIA website
api_url = f'https://api.eia.gov/v2/petroleum/pri/gnd/data/?frequency=weekly&data[0]=value&facets[product][]=EPD2D&facets[product][]=EPMM&facets[product][]=EPMP&facets[product][]=EPMR&facets[series][]=EMD_EPD2D_PTE_NUS_DPG&facets[series][]=EMM_EPMM_PTE_NUS_DPG&facets[series][]=EMM_EPMP_PTE_NUS_DPG&facets[series][]=EMM_EPMR_PTE_NUS_DPG&start={start_date}&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000'
api_key = 'TpnzzxiOIeII7cZuWB2dagJpu9YLNF2WMDqEofb2'

params = {
    'api_key': api_key
}

try:
    response = requests.get(api_url, params=params)
    # This will raise an HTTPError if the response was not successful
    response.raise_for_status()

    # Get the JSON data and put it in a dataframe
    jsonData = response.json()
    df = pd.json_normalize(jsonData, record_path=['response', 'data'])
    df[['period', 'series', 'series-description', 'value']]

    # Changing Column names from api to match sql output table
    column_mapping = {
        'period': 'PublishDate',
        'series': 'SeriesId',
        'series-description': 'Description',
        'value': 'Price'

    }

    df = df.rename(columns=column_mapping)

    # Remove dollars per gallon to match 'description' in Sql
    df['Description'] = df['Description'].str.replace(r" \(.+?\)$", "", regex=True)

    # Adding Columns to match sql output table
    df['Country'] = 'USA'
    df['Frequency'] = 'W'
    df['UnitofMeasure'] = 'Dollars per Gallon'
    df['Source'] = 'EIA, U.S. Energy Information Administration'

    # Creating 'Grade' Column but adding 'weekly' to the end to mathc sql output table
    df['Grade'] = df['Description'].copy()
    df['Grade'] = df['Grade'] + ',Weekly'

    # Get the current date for 'CreatedOn' and 'UpdatedOn' column
    process_date = datetime.now()

    # Adding other columns that are not hardcoded
    df['PublishDate'] = pd.to_datetime(df['PublishDate'])
    df['CreatedOn'] = process_date
    df['UpdatedOn'] = process_date

    # Sort by PublishDate to Rank
    df_sorted = df.sort_values(by='PublishDate', ascending=False)

    # Use Dense so newest publish date will be 1
    df['Id'] = df_sorted['PublishDate'].rank(method='dense', ascending=False).astype(int)

# errors caused by requests
except requests.exceptions.ConnectionError as conn_err:
    print(f'Error Connecting: {conn_err}')
except requests.exceptions.Timeout as timeout_err:
    print(f'Timeout Error: {timeout_err}')
except requests.exceptions.RequestException as req_err:
    print(f'Error: {req_err}')
# error that is not caused by requests
except Exception as e:
    print(f'An error occurred: {e}')
# Connect to Sql Server
server = 'mssql-dev-01'
database = 'Reporting'
username = 'EditorialWrite'
password = 'BBWrite01.'
driver = 'ODBC Driver 17 for SQL Server'

conn_str = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'
engine = sqlalchemy.create_engine(conn_str)

# Write data frame to sql
df.to_sql('core_staging', schema='fuel', con=engine, if_exists='replace', index=False)

# Merge so 'Updated' column will update and output # rows inserted in Fuel_core table
merge_statement = text("""
IF OBJECT_ID('tempdb..##MergeOutput') IS NOT NULL DROP TABLE ##MergeOutput;
CREATE TABLE ##MergeOutput (
    Action VARCHAR(10)
);

MERGE INTO  Reporting.fuel.Core AS target
USING (SELECT PublishDate, Country,Grade,Frequency,Id, Price, CreatedOn, UpdatedOn,UnitofMeasure,Description,Source,SeriesId FROM Reporting.fuel.core_staging) AS source
ON (target.[PublishDate]=source.[PublishDate]
    AND target.[Country]=source.[Country]
    AND target.[SeriesId]=source.[SeriesId]
    AND target.[Frequency]=source.[Frequency])
WHEN MATCHED AND target.Price != source.Price THEN 
    UPDATE SET Price = source.Price,
        UpdatedOn = source.UpdatedOn,
        UnitOfMeasure= source.UnitOfMeasure,
        Grade =source.Grade,
        Description=source.Description,
        Source=source.Source
WHEN NOT MATCHED THEN
    INSERT (PublishDate, Country,Grade,Frequency,Id, Price, CreatedOn, 
        UpdatedOn,UnitofMeasure,Description,Source,SeriesId)
VALUES (source.PublishDate, source.Country,source.Grade,source.Frequency,source.Id, source.Price, source.CreatedOn,
        source.UpdatedOn,source.UnitofMeasure,source.Description,source.Source,source.SeriesId)
OUTPUT $action INTO ##MergeOutput;
""")

count_query = text("SELECT COUNT(*) AS InsertedCount FROM ##MergeOutput WHERE Action = 'INSERT';")

with engine.begin() as conn:
    conn.execute(merge_statement)
    inserted_count_result = conn.execute(count_query).scalar()

    # Drop the global temp table
    conn.execute(text("DROP TABLE IF EXISTS ##MergeOutput;"))

print(f"US Inserted Row Count: {inserted_count_result}")

# Canada

base_url = 'https://www2.nrcan.gc.ca/eneene/sources/pripri/prices_byyear_e.cfm?productID='

# Dictionary mapping fuel types to their product IDs.
CanFuelPrices = {'Regular': 1, 'Mid-Grade': 2, 'Premium': 3, 'Diesel': 5}

# Create a list to store data for all fuel types.
all_fuel_data = []

# Iterate over the dictionary items.
for fuel_type, prod_id in CanFuelPrices.items():
    full_url = f"{base_url}{prod_id}"
    response = requests.get(full_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', id='pricesTable')
        if table:
            # Process each table row
            for row in table.find_all('tr')[1:]:  # Skip header row
                cells = row.find_all('td')
                if cells:
                    # Parse date and filter rows.
                    date_str = cells[0].get_text(strip=True)
                    try:
                        date = datetime.strptime(date_str, '%Y-%m-%d')
                    except ValueError:
                        # Skip rows with invalid date format.
                        continue

                    if date >= two_months_ago:
                        # Assuming the second cell contains the price.
                        price = cells[1].get_text(strip=True)
                        # Append to the list as a dictionary.
                        all_fuel_data.append({
                            'Fuel Type': fuel_type,
                            'Date': date_str,
                            'Price': price
                            # Add other data columns as needed.
                        })
        else:
            print(f"No table with id 'pricesTable' found for {fuel_type}")
    else:
        print(f"Failed to retrieve {fuel_type} data, status code: {response.status_code}")

# Changing Column names from url to match sql output table
grade_mapping = {
    'Regular': 'Gasoline',
    'Mid-Grade': 'Gasoline, Mid-Grade',
    'Premium': 'Gasoline, Premium',
    'Diesel': 'Diesel'
}

# Create a DataFrame from the gathered data.
df = pd.DataFrame(all_fuel_data)

# Divide the 'Price' column by 100 and map grade
df['Price'] = df['Price'].replace('[\$,]', '', regex=True).astype(float)
df['Price'] = df['Price'] / 100
df['Grade'] = df['Fuel Type'].map(grade_mapping)

# Updating the DataFrame's column names to match SQL table's column names
column_mapping = {
    'Date': 'PublishDate'
}

df.rename(columns=column_mapping, inplace=True)
df.drop(columns='Fuel Type', inplace=True)

# Adding Columns to match sql output table
df['Country'] = 'CAN'
df['Frequency'] = 'W'
df['UnitofMeasure'] = 'CAD per litre'
df['Source'] = ''
df['Description'] = ''

# Creating SeriesID column by dupliating 'grade' and making slight adjustments
df['SeriesId'] = df['Grade'].copy()
df['SeriesId'] = 'Canada.' + df['Grade'] + '.Weekly'


def create_series_id(grade):
    grade_formatted = grade.replace(', ', '.')
    if grade == 'Gasoline':
        return 'Canada.Gasoline.Regular.Weekly'
    else:
        return f'Canada.{grade_formatted}.Weekly'


# Apply this function to each row in the 'Grade' column to create the 'SeriesId' column
df['SeriesId'] = df['Grade'].apply(create_series_id)

# Adding other columns that cannot be hardcoded
df['PublishDate'] = pd.to_datetime(df['PublishDate'])
df['CreatedOn'] = process_date
df['UpdatedOn'] = process_date

# Sort by PublishDate for 'Id' column
df_sorted = df.sort_values(by='PublishDate', ascending=False)

# Use Dense so newest publish date will be 1
df['Id'] = df_sorted['PublishDate'].rank(method='dense', ascending=False).astype(int)

# Connect to Sql Server
server = 'mssql-dev-01'
database = 'Reporting'
username = 'EditorialWrite'
password = 'BBWrite01.'
driver = 'ODBC Driver 17 for SQL Server'

conn_str = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'
engine = sqlalchemy.create_engine(conn_str)

# Write data frame to sql
df.to_sql('core_staging', schema='fuel', con=engine, if_exists='replace', index=False)

# Merge so 'Updated' column will update and output # rows inserted in Fuel_core table
merge_statement = text("""
DROP TABLE IF EXISTS ##MergeOutput;
CREATE TABLE ##MergeOutput (
    Action VARCHAR(10)
);

MERGE INTO  Reporting.fuel.Core AS target
USING (SELECT PublishDate, Country,Grade,Frequency,Id, Price, CreatedOn, UpdatedOn,UnitofMeasure,Description,Source,SeriesId FROM Reporting.fuel.core_staging) AS source
ON (target.[PublishDate]=source.[PublishDate]
    AND target.[Country]=source.[Country]
    AND target.[SeriesId]=source.[SeriesId]
    AND target.[Frequency]=source.[Frequency])
WHEN MATCHED AND target.Price != source.Price THEN 
    UPDATE SET Price = source.Price,
        UpdatedOn = source.UpdatedOn,
        UnitOfMeasure= source.UnitOfMeasure,
        Grade =source.Grade,
        Description=source.Description,
        Source=source.Source
WHEN NOT MATCHED THEN
    INSERT (PublishDate, Country,Grade,Frequency,Id, Price, CreatedOn, 
        UpdatedOn,UnitofMeasure,Description,Source,SeriesId)
VALUES (source.PublishDate, source.Country,source.Grade,source.Frequency,source.Id, source.Price, source.CreatedOn,
        source.UpdatedOn,source.UnitofMeasure,source.Description,source.Source,source.SeriesId)
OUTPUT $action INTO ##MergeOutput;
""")

count_query = text("SELECT COUNT(*) AS InsertedCount FROM ##MergeOutput WHERE Action = 'INSERT';")

with engine.begin() as conn:
    conn.execute(merge_statement)
    inserted_count_result = conn.execute(count_query).scalar()

    # Drop the global temp table
    conn.execute(text("DROP TABLE IF EXISTS ##MergeOutput;"))

print(f"Canada Inserted Row Count: {inserted_count_result}")