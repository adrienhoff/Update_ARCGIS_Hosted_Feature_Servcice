# Update_ARCGIS_Hosted_Feature_Servcice
Py script to update, add, and delete values in hosted feature service based on output from SQL DB.

The script compares rows and values in hosted feature service to CSV generated from query layer and updates, adds, or deletes as needed. 

Connection to the original database is triggered by a config.ini created prior to running this script. A config file is attached as example. Replace your values in this file and save as config.ini. 

Authentication to the portal site can be accessed via the GIS(Profile= ' ') function which is created in a the signin script. Replace your values within these scripts. Do not change the name of signin.py. This imports as module so that the function can delete the old profile and recreate if a warning is encountered.

For more information on storing local credentials: https://developers.arcgis.com/python/guide/working-with-different-authentication-schemes/#storing-your-credentials-locally

Store both authentication files to a folder with secure access.

Replace 'your_table', 'your_item_id', 'your_profile', and any other placeholders with your actual database table, feature layer ID, ArcGIS profile, etc. Make sure to fill in the update and delete logic according to your specific requirements.

The bat file can be used to automate the process in NSSM.
