from arcgis.gis import GIS
from arcgis import features, geometry
from arcgis.features import GeoAccessor, GeoSeriesAccessor
import pandas as pd
from copy import copy
from datetime import datetime, date
import time
import configparser
import os
import pyodbc
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.wkt import loads
from shapely.geometry import Point
import numpy as np
import pytz
import warnings
import signin

# Load the config.ini file
config = configparser.ConfigParser()
config.read('config.ini')

# Get the db credentials from the config file
# Retrieve connection parameters
def connect_to_db(config):
    server = config['database']['server']
    database = config['database']['database']
    username = config['database']['username']
    password = config['database']['password']
    conn_str = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'
    return pyodbc.connect(conn_str)

# Function to authenticate to GIS
def authenticate_gis(profile):
    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter("always")
        try:
            gis = GIS(profile=profile)
            print(f"Successfully logged into '{gis.properties.portalHostname}' via the '{gis.properties.user.username}' user")
            return gis
        except Exception as e:
            for warning in caught_warnings:
                if issubclass(warning.category, UserWarning) and "profiles in" in str(warning.message):
                    print("Caught profile warning, setting up profiles again.")
                    signin.setup_profiles()
                    # Retry authentication after setting up profiles
                    gis = GIS(profile=profile)
                    print(f"Successfully logged into '{gis.properties.portalHostname}' via the '{gis.properties.user.username}' user after deleting old profile")
                    return gis
            raise e  # Re-raise exception if it's not the specific warning we are looking for


def query_sql_with_retry(sql_query, conn, retries=3):
    for attempt in range(retries):
        df = pd.read_sql(sql_query, conn)
        if not df.empty:
            return df
        print(f"Retrying SQL query, attempt {attempt + 1} of {retries}...")
        time.sleep(2)  # Brief wait before retry
    print("SQL query returned no records after multiple attempts.")
    return df

datetime_columns = ['yourdatecolumn', 'yourdatecolumn']

def convert_to_UTC(df, columns):
    utc = pytz.UTC
    mst = pytz.timezone('US/Mountain')

    for column in columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors='coerce')  # Parse the dates
            df[column] = df[column].apply(lambda x: mst.localize(x).astimezone(utc) if pd.notnull(x) else x)
    return df

placeholder_date = pd.Timestamp('1900-01-01 00:00:00')



while True:
    try:
        start_time = time.time()
    
        
        # Connect to the database
        conn = pyodbc.connect(conn_str)
        
    
        # Your SQL query
        sql_query = (
            f"SELECT * FROM your_table"
        )
    
        # Read data from SQL query into DataFrame
        df = pd.read_sql(sql_query, conn)
    
        # Check if DataFrame is empty after retries
        if df.empty:
            print("No records retrieved from SQL database. Skipping update cycle.")
            time.sleep(30)  # Wait before retrying the entire loop
            continue
            
        # Convert Latitude, Longitude, and Altitude columns to numeric
        df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
        df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
        df['Zvalue'] = pd.to_numeric(df['Zvalue'], errors='coerce')
    
        # Convert Latitude, Longitude, Altitude to Point geometries
        point_geometry = [Point(xy) if pd.notnull(xy[0]) else None for xy in zip(df['Longitude'], df['Latitude'], df['Zvalue'])]
    
        # Create a GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry=point_geometry)
        gdf.crs = 'EPSG:4326'  
        gdf = gdf.to_crs(epsg=3857)
        gdf = convert_to_UTC(gdf, datetime_columns)
    
        gdf.columns = gdf.columns.str.lower()
    
    
        # Replace NaN values in the GeoDataFrame with None or empty strings
        nan_replacements = {
            'address': '',
            'zip': '',
            # Add other columns here
        }
    
        # Replace NaN values with None using NumPy
        for column, replacement in nan_replacements.items():
                gdf[column].replace({np.nan: replacement}, inplace=True)
    
        # Close DB connection
        conn.close()
        
        # Access the feature layer
        item = gis.content.get("your_item_id")
        feature_layer = item.layers[0]
    
        # Query the feature layer
        incident_fset = feature_layer.query() 
        incident_df = incident_fset.sdf  
    
        incident_gdf = gpd.GeoDataFrame(incident_df, geometry ='SHAPE')
        incident_gdf.crs = 'EPSG:3857'
        spatial_reference = incident_gdf.crs
    
        incident_gdf = convert_to_UTC(incident_gdf, datetime_columns)
    
        # Merge dataframes
        overlap_rows = pd.merge(left=incident_df, right=gdf, how='inner', on='your_uid')
    
        features_for_update = [] #list containing corrected features
        all_features = incident_fset.features
        #all_features[0]
    
        batch_size = 100  # Adjust the batch size as needed
    
        # Print the columns and data types to verify changes
        print("Columns and data types of gdf after conversions:")
        print(gdf.info())
    
        print("\nColumns and data types of incident_gdf after conversions:")
        print(incident_gdf.info())
    
    
        if not overlap_rows.empty:
            for i in range(0, len(overlap_rows), batch_size):
                    batch_rows = overlap_rows.iloc[i:i+batch_size]
        
                
                for master_incident_number in overlap_rows['master_incident_number']:
                    original_feature = [f for f in all_features if f.attributes['youruid'] == youruid][0] #replace youruid with your uid
                    feature_to_be_updated = copy(original_feature)
    
                    matching_rows = gdf[gdf['master_incident_number'] == master_incident_number]
                        if not matching_rows.empty:
                            # Access the first row using .iloc[0]
                            matching_row = matching_rows.iloc[0]
                            
    
                            # Get the input geometry
                            input_geometry = matching_row['geometry']
                            input_geometry_dict = {'x': input_geometry.x, 'y': input_geometry.y}
                            feature_to_be_updated.geometry = input_geometry_dict
    
                            
                            if pd.to_datetime(matching_row['your_date_column']) == placeholder_date:
                                gdf.loc[matching_row.name, 'your_date_column'] = pd.NaT
                            if pd.to_datetime(matching_row['your_date_column2']) == placeholder_date:
                                gdf.loc[matching_row.name, 'your_date_column2'] = pd.NaT
    
                            feature_to_be_updated.attributes.update({
                                'zip': str(matching_row['zip']),  
                                'your date column': matching_row['your date column'] if pd.notnull(matching_row['your date column']) else None,
                                #CONTINUE LOGIC
    
                            features_for_update.append(feature_to_be_updated)
                feature_layer.edit_features(updates=features_for_update)
                    
                # Print progress
                print("Updating batch {} out of {} batches. Batch size: {}.".format((i//batch_size)+1, (len(overlap_rows)//batch_size)+1, len(batch_rows)))
    
            print("All batches updated successfully.")
        else:
            print("No features to update")
    
    
        # Select new rows
        new_rows = gdf[~gdf['your_uid'].isin(incident_df['your_uid'])]
    
        print(new_rows.shape)
        new_rows.head()
    
    
        features_to_be_added = []
        print("Number of rows in new_rows:", len(new_rows))  # Diagnostic print statement
    
        for index, row in new_rows.iterrows():
            new_feature = {
                "attributes": {},  
                "geometry": {
                    "x": row['geometry'].x,
                    "y": row['geometry'].y
                }
        }

            if pd.to_datetime(row['your_date_column']) == placeholder_date:
                gdf.loc[row.name, 'your_date_column'] = pd.NaT
            if pd.to_datetime(row['your_date_column2']) == placeholder_date:
                gdf.loc[row.name, 'your_date_column2'] = pd.NaT
                
            # Access data in the Series using string or int indices 
            #example
            address = row['address']
            zip_code = str(row['zip'])
            your_date_column = row['your_date_column'] if pd.notnull(row['your_date_column']) else None

            #continue with this logic for your other fields
            
            # Assign the updated values to attributes dictionary
            new_feature["attributes"]['address'] = address
            new_feature["attributes"]['zip'] = zip_code
            new_feature["attributes"]['your_date_column2'] = your_date_column2
            #continue with this logic for your other fields
    
    
            # Append this feature to the list of features to be added
            features_to_be_added.append(new_feature)
            
    
        if features_to_be_added:
            feature_layer.edit_features(adds=features_to_be_added)
            print("Features added successfully.")  # Diagnostic print statement
    
            incident_fset = feature_layer.query()  # Querying without any conditions returns all the features
            print("Number of features after addition:", len(incident_fset.features))  # Check if features were added
        else:
            print("No features to add.")
    
    
        # Delete rows
        incident_gdf.shape
        delete_rows = incident_df[~incident_df['your_uid'].isin(gdf['your_uid'])]
    
        #print(delete_rows.shape)
        delete_rows.head()
    
        incident_fset = feature_layer.query()  # Querying without any conditions returns all the features
        print("Number of features returned by the query:", len(incident_fset.features))
    
        if not delete_rows.empty:
            features_to_delete = [feature for feature in incident_fset.features if feature.attributes['your_uid'] in delete_rows['master_incident_number'].values]
    
            if features_to_delete:
                object_ids_to_delete = [feature.attributes['objectid1'] for feature in features_to_delete]
                print("Number of features selected for deletion:", len(features_to_delete))
                print("OBJECTIDs to delete:", object_ids_to_delete)
                delete_results = feature_layer.edit_features(deletes=object_ids_to_delete)
                print(delete_results)
                print("Features deleted successfully.")
            else:
                print("No features to delete.")
        else:
            print("No features to delete.")
    
        incident_fset = feature_layer.query()  # Querying without any conditions returns all the features
        print("Number of features returned by the query:", len(incident_fset.features))
    
    
        end_time = time.time()
        total_time = end_time - start_time
        print("Total time taken for the script to run: {:.2f} seconds".format(total_time))

        time.sleep(60)
    
    except Exception as e:
        # Print the error message
        print("An error occurred:", e)
        print("Restarting the script...")

        # Wait for a few seconds before restarting the script
        time.sleep(5)       
