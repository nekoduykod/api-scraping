import requests
import pandas as pd
import sqlite3
from datetime import date, timedelta
from bs4 import BeautifulSoup
import json

current_date = date.today()
previous_dates = current_date - timedelta(days=2)

current_date_str = current_date.strftime("%Y-%m-%d")
previous_dates_str = previous_dates.strftime("%Y-%m-%d")

url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/installs"

installs_params = {"date": previous_dates_str}

headers = {"Authorization": "***_0cRdtmCdCu9XFkFNs45dpTw3NhQ7ysIwYIn8EEZzpal1uUWdkR0TXgGkY0SXfehCy-4rUZh81Hr3PaZckxyJp3VIcgBzk8qGEpZRMD8_KBJukbtVYkaobYX7jMv4f2TA0kbXkCADTM2yCJw=="}

with sqlite3.connect('data_marts.db') as conn:
    data_frames = []
    for url, params in [(url, installs_params)]:
        try:
            print("URL:", url)
            print("Params:", params)

            response = requests.get(url, params=params, headers=headers)
            print("Status Code:", response.status_code)

            content_type = response.headers.get('Content-Type', '')

            # Check if the content is HTML
            if 'text/html' in content_type:
                soup = BeautifulSoup(response.content, 'html.parser')
                # Extract the HTML content as a string, remove curly braces, and convert to list of dictionaries
                data_str = soup.get_text().replace('{','').replace('}','')
                data_str = '[' + data_str.replace('}, {', '}, {') + ']'  # Wrap in brackets and add space after commas
                data_list = json.loads(data_str)
                # Convert the list of dictionaries into a DataFrame
                df = pd.DataFrame(data_list)
                data_frames.append(df)
            else:
                print(f"Received unsupported content from {url}. Content Type: {content_type}")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while making the request to {url}: {e}")

    if data_frames:
        combined_dataframe = pd.concat(data_frames, ignore_index=True)
        
        # Check if the DataFrame has columns
        if not combined_dataframe.empty:
            # Create or replace the table with appropriate columns
            combined_dataframe.to_sql('installs_table', conn, if_exists='replace', index=False)
            combined_dataframe.to_csv('installs.csv', index=False)
        else:
            print("DataFrame has no columns, no data to write to the table.")
    else:
        print("No data to concatenate.")
        
'''error : json.decoder.JSONDecodeError: Expecting ',' delimiter: line 1 column 9 (char 8) '''




''' this below says "sqlite3.OperationalError: too many columns on installs_table" '''

# import requests
# import pandas as pd
# import sqlite3
# from datetime import date, timedelta
# from bs4 import BeautifulSoup

# current_date = date.today()
# previous_dates = current_date - timedelta(days=2)

# current_date_str = current_date.strftime("%Y-%m-%d")
# previous_dates_str = previous_dates.strftime("%Y-%m-%d")

# url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/installs"

# installs_params = {"date": previous_dates_str}

# headers = {"Authorization": "gAAAAABlcEioZ8CObY_0cRdtmCdCu9XFkFNs45dpTw3NhQ7ysIwYIn8EEZzpal1uUWdkR0TXgGkY0SXfehCy-4rUZh81Hr3PaZckxyJp3VIcgBzk8qGEpZRMD8_KBJukbtVYkaobYX7jMv4f2TA0kbXkCADTM2yCJw=="}

# with sqlite3.connect('data_marts.db') as conn:
#     data_frames = []
#     for url, params in [(url, installs_params)]:
#         try:
#             print("URL:", url)
#             print("Params:", params)

#             response = requests.get(url, params=params, headers=headers)
#             print("Status Code:", response.status_code)

#             content_type = response.headers.get('Content-Type', '')

#             # Check if the content is HTML
#             if 'text/html' in content_type:
#                 soup = BeautifulSoup(response.content, 'html.parser')
#                 # Extract and process the HTML data into a DataFrame
#                 # Modify the following logic based on the actual structure of your HTML
#                 data = soup.get_text()  # Extract the text from the HTML
#                 data = data.replace('{','').replace('}','')  # Remove the curly braces
#                 # Now you can process the cleaned data into a DataFrame
#                 df = pd.DataFrame([data.split(",")])  # Split the data by commas and convert into a DataFrame
#                 data_frames.append(df)
#             else:
#                 print(f"Received unsupported content from {url}. Content Type: {content_type}")
#         except requests.exceptions.RequestException as e:
#             print(f"An error occurred while making the request to {url}: {e}")

#     if data_frames:
#         combined_dataframe = pd.concat(data_frames, ignore_index=True)
        
#         # Check if the DataFrame has columns
#         if not combined_dataframe.empty:
#             # Create or replace the table with appropriate columns
#             combined_dataframe.to_sql('installs_table', conn, if_exists='replace', index=False)
#             combined_dataframe.to_csv('installs.csv', index=False)
#         else:
#             print("DataFrame has no columns, no data to write to the table.")
#     else:
#         print("No data to concatenate.")


'''data that is here '''
# {"install_time":"2023-12-05T23:43:41.168000","marketing_id":"OK90Ygms2Djt","channel":"Bing","medium":"referral",
# "campaign":"SignUpBonus","keyword":"signup now","ad_content":"video-ad","ad_group":"None","landing_page":"Undefined",
# "sex":"male","alpha_2":"CH","alpha_3":"CHE","flag":"\ud83c\udde8\ud83c\udded","name":"Switzerland","numeric":"756",
# "official_name":"Swiss Confederation"},{"install_time":"2023-12-05T23:47:34.682000","marketing_id":"Ol5BDc6tvxN2",
# "channel":"Organic Search","medium":"email","campaign":"SpringSale","keyword":"None","ad_content":"video-ad","ad_group":"None","landing_page":"Undefined",
#  "sex":"male","alpha_2":"DE","alpha_3":"DEU","flag":"\ud83c\udde9\ud83c\uddea","name":"Germany","numeric":"276",
# "official_name":"Federal Republic of Germany"},{"install_time":"2023-12-05T23:48:16.635000","marketing_id":"OPLT6l9WHRa9",
#  "channel":"Google","medium":"social","campaign":"SpecialOffer","keyword":"None","ad_content":"banner1","ad_group":"None",
#  "landing_page":"Undefined","sex":"female","alpha_2":"DE","alpha_3":"DEU","flag":"\ud83c\udde9\ud83c\uddea","name":"Germany",
#  "numeric":"276","official_name":"Federal Republic of Germany"}, etc