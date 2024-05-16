from arcgis.gis import GIS
from arcgis.features import GeoAccessor
import pandas as pd
from copy import copy
import time
import configparser
import pyodbc
import geopandas as gpd
from shapely.geometry import Point
import numpy as np

# Define batch size
batch_size = 100

while True:
    start_time = time.time()

    # Load the config.ini file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Get the db credentials from the config file
    # Retrieve connection parameters
    server = config['database']['server']
    database = config['database']['database']
    username = config['database']['username']
    password = config['database']['password']

    conn_str = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'

    print("Authenticated to db successfully!")

    # Connect to the database
    conn = pyodbc.connect(conn_str)

    # Your SQL query
    sql_query = (
        f"SELECT * FROM your_table"
    )

    # Read data from SQL query into DataFrame
    df = pd.read_sql(sql_query, conn)

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

    # Replace NaN values in the GeoDataFrame with None or empty strings
    nan_replacements = {
        'address': '',
        'zip': '',
        # Add other columns here
    }

    # Replace NaN values with None using NumPy
    gdf = gdf.fillna(nan_replacements)

    # Close DB connection
    conn.close()

    # Authenticate to ArcGIS Online or ArcGIS Enterprise
    gis = GIS(profile='your_profile')
    
    # Access the feature layer
    item = gis.content.get("your_item_id")
    feature_layer = item.layers[0]

    # Query the feature layer
    incident_fset = feature_layer.query() 
    incident_df = incident_fset.sdf  

    # Merge dataframes
    overlap_rows = pd.merge(left=incident_df, right=gdf, how='inner', on='your_uid')

    if not overlap_rows.empty:
        features_for_update = [] 
        
        for i in range(0, len(overlap_rows), batch_size):
            batch_rows = overlap_rows.iloc[i:i+batch_size]
            
            for master_incident_number in overlap_rows['master_incident_number']:
                # Your update logic here
                
            # Print progress
            print("Updating batch {} out of {} batches. Batch size: {}.".format((i//batch_size)+1, (len(overlap_rows)//batch_size)+1, len(batch_rows)))

        print("All batches updated successfully.")
    else:
        print("No features to update")

    print("========================================================================")

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

            
        # Access data in the Series using string or int indices 
        #example
        address = row['address']
        zip_code = str(row['zip'])
        #continue with this logic for your other fields
        
        # Assign the updated values to attributes dictionary
        new_feature["attributes"]['address'] = address
        new_feature["attributes"]['zip'] = zip_code
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
