# Що заводно можна писати тут

import requests
import pandas as pd
import sqlite3
from datetime import date, timedelta
from bs4 import BeautifulSoup

current_date = date.today()
previous_dates = current_date - timedelta(days=2)

current_date_str = current_date.strftime("%Y-%m-%d")
previous_dates_str = previous_dates.strftime("%Y-%m-%d")

url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/installs"

installs_params = {"date": previous_dates_str}

headers = {"Authorization": "gAAAAABlcEioZ8CObY_0cRdtmCdCu9XFkFNs45dpTw3NhQ7ysIwYIn8EEZzpal1uUWdkR0TXgGkY0SXfehCy-4rUZh81Hr3PaZckxyJp3VIcgBzk8qGEpZRMD8_KBJukbtVYkaobYX7jMv4f2TA0kbXkCADTM2yCJw=="}

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
                # Extract and process the HTML data into a DataFrame
                # Modify the following logic based on the actual structure of your HTML
                data = soup.get_text()  # Extract the text from the HTML
                data = data.replace('{','').replace('}','')  # Remove the curly braces
                # Now you can process the cleaned data into a DataFrame
                df = pd.DataFrame([data.split(",")])  # Split the data by commas and convert into a DataFrame
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


# ------------------------------------------------------------------
# installs_response = requests.get(url, params=installs_params, headers=headers)   
# costs_response = requests.get(url, params=costs_params, headers=headers)
# orders_response = requests.get(url, params=orders_params, headers=headers)
# events_response = requests.get(url, params=events_params, headers=headers)
 
# installs_data = installs_response.json()
# costs_data = costs_response.json()
# orders_data = orders_response.json()
# events_data = events_response.json()

# df1 = pd.DataFrame(installs_data)
# df2 = pd.DataFrame(costs_data)
# df3 = pd.DataFrame(orders_data)
# df4 = pd.DataFrame(events_data)

# df_total = pd.contact([df1, df2, df3, df4])
# print(df_total.columns)
# print(df_total.head())
# print(df_total.shape) 

# df_total.to_sql('data_mart', conn, if_exists='replace')
# ------------------------------------------------------------------

# ------------------------------------------------------------------
# If memory usage is a concern or if you prefer to keep data processing separate from database operations, 
# the first snippet (using data_frames list) may be more suitable.
# If you are working with smaller datasets and prefer to write to the database immediately, 
# the second snippet may be more straightforward.
# for url, params in [(installs_url, installs_params), (costs_url, costs_params), 
#                     (orders_url, orders_params), (events_url, events_params)]:
#    response = requests.get(url, params=params, headers=headers)
#    data = response.json()
  
#    # Handle pagination
#    while 'next_page' in data:                               
#        params['next_page'] = data['next_page']
#        response = requests.get(url, params=params, headers=headers)
#        data = response.json()

#    df = pd.DataFrame(data)
#    df.to_sql('data_mart', conn, if_exists='replace', index=False) 
# ------------------------------------------------------------------