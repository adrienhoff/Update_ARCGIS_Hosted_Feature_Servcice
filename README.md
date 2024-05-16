# Update_ARCGIS_Hosted_Feature_Servcice
Py script to update, add, and delete values in hosted feature service based on output from SQL DB.

The script compares rows and values in hosted feature service to CSV generated from query layer and updates, adds, or deletes as needed. 

Connection to the original database is triggered by a config.ini created prior to running this script.

Authentication to the portal site can be accessed via the GIS(Profile= ' ') function which is also created in a separate script.
For more information on storing local credentials: https://developers.arcgis.com/python/guide/working-with-different-authentication-schemes/#storing-your-credentials-locally

Replace 'your_table', 'your_item_id', 'your_profile', and any other placeholders with your actual database table, feature layer ID, ArcGIS profile, etc. Make sure to fill in the update and delete logic according to your specific requirements.
