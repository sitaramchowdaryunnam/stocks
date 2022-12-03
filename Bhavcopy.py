import requests
import zipfile
from jugaad_data.nse import bhavcopy_save
from datetime import date


bhavcopy_save(date(2022,12,2), "")


# url = 'https://www1.nseindia.com/content/historical/EQUITIES/2022/DEC/cm02DEC2022bhav.csv.zip'
# r = requests.get(url, stream=True)

# with open("cm02DEC2022bhav.csv.zip" , "wb") as file:
#     for chunck in r.iter_content(chunk_size=1024):
#         file.write(chunck)

# with zipfile.ZipFile("cm02DEC2022bhav.csv.zip" , "r")  as c_file:
#     c_file.extractall("")