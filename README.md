# Update_ARCGIS_Hosted_Feature_Servcice
Py script to update, add, and delete values in hosted feature service based on output from SQL DB.

The script compares rows and values in hosted feature service to CSV generated from query layer and updates, adds, or deletes as needed. 

The query layer is created from an SDE connection file which must be generated before running the script.

The mentioned config file store portal authentication can be created in a text file and saved as config.ini. This file must contain the following:
[portal]
url = your/url
username = your_username
password = your_password
