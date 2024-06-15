import os
import pandas as pd
import base64
import pickle
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# Load your existing report CSV file
report_file = 'C:/Users/muniv/Desktop/Market/marketdata_analysis/Reports_gen_multi_d30-08-2023__Test_with_URL.csv'
df = pd.read_csv(report_file)

# Convert 'Entry Date' to datetime format, specify the format
df['Entry Date'] = pd.to_datetime(df['Entry Date'], format='%d-%m-%Y')

# Sort dataframe by 'Entry Date'
df_sorted = df.sort_values('Entry Date')

# Get the unique dates and select the last two
unique_dates = df_sorted['Entry Date'].unique()
last_two_dates = unique_dates[-2:]

# Filter DataFrame to include only rows where 'Entry Type' is 'Golden entry' 
# and 'Entry Date' is within the last two unique dates
df_filtered = df_sorted[(df_sorted['Entry Type'] == 'Golden entry') & (df_sorted['Entry Date'].isin(last_two_dates))]

# Sort the filtered DataFrame in descending order by 'Entry Date'
df_filtered = df_filtered.sort_values('Entry Date', ascending=False)

SCOPES = ["https://www.googleapis.com/auth/gmail.send", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/spreadsheets"]

# Load previously obtained credentials from token.pickle
creds = None
if os.path.exists("token.pickle"):
    with open("token.pickle", 'rb') as token:
        creds = pickle.load(token)

# If there are no (valid) credentials available, let the user log in.
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
        creds = flow.run_local_server(port=0)

    # Save the credentials for the next run
    with open("token.pickle", "wb") as token:
        pickle.dump(creds, token)

service = build("gmail", "v1", credentials=creds)

sender = 'muni.vadlamudi47@gmail.com'
to = ['muni.vadlamudi47@gmail.com'] 
bcc = ['muni.vadlamudi47@gmail.com']
subject = f"Daily Report - {datetime.now().strftime('%Y-%m-%d')}"
message_text = "Please check the attached report for daily analysis. \n This is purely for educational and personal viewing, and not a recommendation for purchasing."

def create_message_with_df(sender, to, bcc, subject, message_text, df):
    # Convert the DataFrame to HTML
    df_html = df.to_html()

    # Create the email message
    message = MIMEMultipart()
    message["to"] = to
    message["bcc"] = bcc
    message["subject"] = subject

    # Create the email body
    msg = MIMEText(f"{message_text}\n\n{df_html}", "html")
    message.attach(msg)

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
    return {"raw": raw_message}

# Set the 'to' field directly in the message dictionary
message = create_message_with_df(sender, ', '.join(to), ', '.join(bcc) ,subject, message_text, df_filtered)

try:
    sent_message = (
        service.users().messages().send(userId="me", body=message).execute()
    )
    print(f"Sent message Message Id: {sent_message['id']}")
except HttpError as error:
    print(f"An error occurred: {error}")

# Google Drive authentication
gauth = GoogleAuth()
gauth.DEFAULT_SETTINGS['client_config_file'] = "client_secrets.json"  # Explicitly set the client secrets file

# Try to load saved credentials
if os.path.exists("drive_token.pickle"):
    with open("drive_token.pickle", "rb") as token:
        gauth.LoadCredentialsFile("drive_token.pickle")

# Check if the credentials need to be refreshed
if gauth.credentials is None or gauth.access_token_expired:
    gauth.LocalWebserverAuth()  # Creates a local webserver for authentication
    # Save the credentials for the next run
    gauth.SaveCredentialsFile("drive_token.pickle")

drive = GoogleDrive(gauth)

# Specify the name of the Google Sheets file
file_name = 'Reports_gen_multi_d30-08-2023__Test_with_URL'

# Find the Google Sheets file by name
file_list = drive.ListFile({'q': f"title contains '{file_name}' and mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"}).GetList()
if not file_list:
    raise FileNotFoundError(f"The file '{file_name}' was not found in Google Drive.")
    
file_drive = file_list[0]
spreadsheet_id = file_drive['id']

# Build the Sheets API service
sheets_service = build('sheets', 'v4', credentials=creds)

# Clear the existing data in the sheet
clear_values_request_body = {}

request = sheets_service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range='Sheet1', body=clear_values_request_body)
response = request.execute()
# Find the Google Sheets file by name
file_list = drive.ListFile({'q': f"title contains '{file_name}' and mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"}).GetList()
print("Found files:")
for file in file_list:
    print(f"{file['title']} - {file['id']}")
if not file_list:
    raise FileNotFoundError(f"The file '{file_name}' was not found in Google Drive.")

# Update the sheet with new data
def update_sheet(sheet_id, range_name, df):
    values = [df.columns.values.tolist()] + df.values.tolist()
    body = {
        'values': values
    }
    result = sheets_service.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=range_name,
        valueInputOption='RAW', body=body).execute()
    print(f'{result.get("updatedCells")} cells updated.')

update_sheet(spreadsheet_id, 'Sheet1', df_filtered)
