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
df['Entry Date'] = pd.to_datetime(df['Entry Date'], format='%Y-%m-%d')

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

SCOPES = ["https://www.googleapis.com/auth/gmail.send", "https://www.googleapis.com/auth/drive.file"]

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
message = create_message_with_df(sender, ', '.join(to), ', '.join(bcc), subject, message_text, df_filtered)

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

# Get the 'Market Tracker' folder ID
folder_list = drive.ListFile({'q': "title = 'Market Tracker' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"}).GetList()
if not folder_list:
    print("The 'Market Tracker' folder was not found.")
else:
    folder_id = folder_list[0]['id']

    # Check if the file already exists in the 'Market Tracker' folder
    file_list = drive.ListFile({'q': f"'{folder_id}' in parents and title = '{os.path.basename(report_file)}' and trashed = false"}).GetList()
    if file_list:
        # If the file exists, update it
        file_drive = file_list[0]
        file_drive.SetContentFile(report_file)
        file_drive.Upload()
        print(f'Updated file: {file_drive["title"]}')
    else:
        # If the file does not exist, create it in the 'Market Tracker' folder
        file_drive = drive.CreateFile({'title': os.path.basename(report_file), 'parents': [{'id': folder_id}]})
        file_drive.SetContentFile(report_file)
        file_drive.Upload()
        print(f'Uploaded file: {file_drive["title"]}')
