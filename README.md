# Update_ARCGIS_Hosted_Feature_Servcice
Py script to update, add, and delete values in hosted feature service based on output from SQL DB.

The script compares rows and values in hosted feature service to CSV generated from query layer and updates, adds, or deletes as needed. 

The query layer is created from an SDE connection file which must be generated before running the script.

Authentication to the portal site can be accessed via the GIS(Profile= ' ') function which is also created in a separate script.
For more information on storing local credentials: https://developers.arcgis.com/python/guide/working-with-different-authentication-schemes/#storing-your-credentials-locally
