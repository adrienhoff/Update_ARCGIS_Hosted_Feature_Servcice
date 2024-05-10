from arcgis.gis import GIS
from arcgis import features
import pandas as pd
from arcgis import geometry #use geometry module to project Long,Lat to X and Y
from copy import copy
from cooy import deepcopy
from datetime import datetime
import configparser
import os

# Load portal auth configuration from config.ini
config = configparser.ConfigParser()
config.read('config.ini') #replace with your config file name

# Get portal URL, username, and password from the config file
portal_url = config['portal']['url']
username = config['portal']['username']
password = config['portal']['password']

# Initialize GIS object
gis = GIS(portal_url, username, password)

#create query layer from SDE conection file
with arcpy.EnvManager(scratchWorkspace=r"C:\GIS\Default.gdb", workspace=r"C:\GIS\Default.gdb"):
    arcpy.management.MakeQueryLayer(r"your\connection\file", "your_output", "select * from DB_SCHEMA_TABLE", ...) #continue with your  values
#create csv from query layer
with arcpy.EnvManager(scratchWorkspace=r"C:\GIS\Default.gdb", workspace=r"C:\GIS\Default.gdb"):
    arcpy.conversion.TableToTable("your_querylayer_output", "your_folder", "yourcsv.csv", '', ..) #continue with your values
    
csv1 = 'yourcsv.csv'
df_1 = pd.read_csv(csv1)
df_1.columns = df_1.columns.str.lower()
df_1.head()

df_1.shape

item = gis.content.get("your feature item number")
feature_layer = item.layers[0]

incident_fset = feature_layer.query() # Querying without any conditions returns all the features
print("Number of features returned by the query:", len(incident_fset.features))

incident_df = incident_fset.sdf  # Convert feature set to pandas DataFrame
incident_df.head()  # Display the first few rows of the DataFrame


overlap_rows = pd.merge(left = incident_fset.sdf, right = df_1, how='inner',
                       on = 'your_unique_id') #replace with uiq value
print("Number of features overlapped:", len(overlap_rows))
overlap_rows


features_for_update = [] #list containing corrected features
all_features = incident_fset.features
all_features[0]


for master_incident_number in overlap_rows['your_unique_id']:
    original_feature = [f for f in all_features if f.attributes['your_unique_id'] == master_incident_number][0]
    feature_to_be_updated = copy(original_feature)
    
    print(str(original_feature))
    
    # get the matching row from csv
    matching_row = df_1[df_1['your_unique_id'] == master_incident_number].iloc[0]
    

    # Convert response_date to datetime object
    response_date_str = matching_row['response_date']
    response_date = datetime.strptime(response_date_str, '%m/%d/%Y %H:%M:%S')
    
    latitude = float(matching_row['latitude'])
    longitude = float(matching_row['longitude'])
    
    #get geometries in the destination coordinate system
    input_geometry = {'y': matching_row['latitude'], 'x': matching_row['longitude']}
    output_geometry = geometry.project(geometries=[input_geometry], in_sr=4326, out_sr=incident_fset.spatial_reference['latestWkid'], gis=gis)
    
    # assign the updated values
    feature_to_be_updated.geometry = output_geometry[0]    
    feature_to_be_updated.attributes['address']=str(matching_row['address'])
    #... continue with field map values
    
    features_for_update.append(feature_to_be_updated)
                                                          
                                                     

feature_layer.edit_features(updates= features_for_update)

#select those rows hat do not overlap 
new_rows = df_1[~df_1['your_unique_id'].isin(overlap_rows['your_unique_id'])]
print(new_rows.shape)
new_rows.head()

features_to_be_added = []
print("Number of rows in new_rows:", len(new_rows))  # Diagnostic print statement

if not new_rows.empty:
    template_feature = deepcopy(features_for_update[0])
    
    for index, row in new_rows.iterrows():
        new_feature = deepcopy(template_feature)
        
        # Access data in the Series using string indices
        address = row['address']
        #continue with field map

        # Assign the updated values
        new_feature.attributes['address'] = address
        #continue with field map

        # Add this to the list of features to be added
        features_to_be_added.append(new_feature)

    print("Features to be added:", features_to_be_added)  # Diagnostic print statement
    
    if features_to_be_added:
        feature_layer.edit_features(adds=features_to_be_added)
        print("Features added successfully.")  # Diagnostic print statement

print("End of adds update.")  # Diagnostic print statement


# Now perform deletes
print(incident_fset.sdf.shape) 
delete_rows = incident_fset.sdf[~incident_fset.sdf['your_unique_id'].isin(overlap_rows['your_unique_id'])]

print(delete_rows.shape)
delete_rows.head()

incident_fset = feature_layer.query()  # Querying without any conditions returns all the features
print("Number of features returned by the query:", len(incident_fset.features))

if not delete_rows.empty:
    features_to_delete = [feature for feature in incident_fset.features if feature.attributes['your_unique_id'] in delete_rows['your_unique_id'].values]

    if features_to_delete:
        object_ids_to_delete = [feature.attributes['objectid'] for feature in features_to_delete]
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



