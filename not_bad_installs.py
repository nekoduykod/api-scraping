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

                # Find and extract the JSON-like text
                json_text = soup.get_text(strip=True)

                # Remove leading and trailing characters that are not part of the JSON
                json_text = json_text.lstrip('window.data =').rstrip(';')

                # Load JSON data
                json_data = json.loads(json_text)

                # Normalize the JSON data into a DataFrame
                df = pd.json_normalize(json_data)

                # Append the DataFrame to the list
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
            
            # Write the DataFrame to a CSV file
            combined_dataframe.to_csv('not_bad_installs_data.csv', index=False)
        else:
            print("DataFrame has no columns, no data to write to the table.")
    else:
        print("No data to concatenate.")
