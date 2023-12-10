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

headers = {"Authorization": "****_0cRdtmCdCu9XpalVYka"}

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
        
 