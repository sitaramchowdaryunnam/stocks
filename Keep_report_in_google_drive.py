from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os

# Authenticate the user
gauth = GoogleAuth()
gauth.LocalWebserverAuth()  # Creates a local webserver for authentication

# Create Google Drive instance
drive = GoogleDrive(gauth)

# Path to the existing file on your laptop
file_path = 'C:/Users/muniv/Desktop/Market/marketdata_analysis/Reports_gen_multi_d30-08-2023__Test_with_URL.csv'
file_name = os.path.basename(file_path)

# Check if the file already exists in Google Drive
file_list = drive.ListFile({'q': f"title = '{file_name}' and trashed = false"}).GetList()
if file_list:
    # If the file exists, update it
    file_drive = file_list[0]
    file_drive.SetContentFile(file_path)
    file_drive.Upload()
    print(f'Updated file: {file_drive["Analysis Report"]}')
else:
    # If the file does not exist, create it
    file_drive = drive.CreateFile({'Analysis Report': file_name})
    file_drive.SetContentFile(file_path)
    file_drive.Upload()
    print(f'Uploaded file: {file_drive["Analysis Report"]}')
