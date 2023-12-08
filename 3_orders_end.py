import requests
import pandas as pd
import sqlite3
import json   # Since BeautifulSoup added, i think json, codecs to be removed. 
import codecs
from datetime import date, timedelta
from bs4 import BeautifulSoup

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

            # Checks if the content is JSON  <= perhaps this snippet => useless 
            if 'application/json' in content_type:  
                data = response.json()

                if not data or 'data' not in data:
                    print(f"No data received from {url}.")
                    continue

                df = pd.json_normalize(data['data'])
                data_frames.append(df)
            # Checks if the content is HTML
            elif 'text/html' in content_type:
                soup = BeautifulSoup(response.content, 'html.parser')
                # Extract and process the HTML data into a DataFrame
                # Modify the following logic based on the actual structure of your HTML
                # For example, you might need to find the table or specific elements
                # and extract data accordingly.
                data = []  # Placeholder, replace with actual HTML parsing logic
                df = pd.DataFrame(data)
                data_frames.append(df)
            else:
                print(f"Received unsupported content from {url}. Content Type: {content_type}")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while making the request to {url}: {e}")

    if data_frames:
        combined_dataframe = pd.concat(data_frames, ignore_index=True)
        combined_dataframe.to_csv('orders_data.csv', index=False)
        combined_dataframe.to_sql('orders_table', conn, if_exists='replace', index=False)
    else:
        print("No data to concatenate.")
