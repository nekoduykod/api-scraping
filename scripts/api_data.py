import requests
import pandas as pd
import sqlite3
from datetime import date, timedelta
import json
import codecs

current_date = date.today()
previous_dates = current_date - timedelta(days=2)

current_date_str = current_date.strftime("%Y-%m-%d")
previous_dates_str = previous_dates.strftime("%Y-%m-%d")

url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/"

installs_url = url + "installs"
costs_url = url + "costs"
orders_url = url + "orders"
events_url = url + "events"

installs_params = {"date": previous_dates_str}
costs_params = {"date": previous_dates_str, "dimensions": "location,channel,medium,campaign,keyword,ad_content,ad_group,landing_page"}
orders_params = {"date": previous_dates_str}
events_params = {"date": previous_dates_str}

headers = {"Authorization": "***_0cRdtmCdCu9XFkFNs45dpTw3NhQ7ysIwYIn8EEZzpal1uUWdkR0TXgGkY0SXfehCy-4rUZh81Hr3PaZckxyJp3VIcgBzk8qGEpZRMD8_KBJukbtVYkaobYX7jMv4f2TA0kbXkCADTM2yCJw=="}

with sqlite3.connect('data_mart.db') as conn:
    data_frames = []

    for url, params in [(installs_url, installs_params), (costs_url, costs_params),
                        (orders_url, orders_params), (events_url, events_params)]:
        try:
            print("URL:", url)
            print("Params:", params)

            response = requests.get(url, params=params, headers=headers)
            print("Status Code:", response.status_code)

            try:
                # Attempt to decode using utf-8
                content = response.content.decode('utf-8')
            except UnicodeDecodeError:
                # If decoding with utf-8 fails, try another encoding
                content = response.content.decode('ISO-8859-1')

            # Use codecs to handle Unicode escape sequences
            cleaned_content = codecs.decode(content, 'unicode_escape')

            print("Response Content:", cleaned_content)
            print("Content Type:", response.headers.get('Content-Type'))
            response.raise_for_status()

            if cleaned_content and response.headers['Content-Type'] == 'application/json':
                data = json.loads(cleaned_content)
                if not data or 'data' not in data:
                    print(f"No data received from {url}.")
                    continue

                df = pd.json_normalize(data['data'])
                data_frames.append(df)

                while 'next_page' in data:
                    response = requests.get(url, params=params, headers=headers)
                    response.raise_for_status()

                    try:
                        # Attempt to decode using utf-8
                        content = response.content.decode('utf-8')
                    except UnicodeDecodeError:
                        # If decoding with utf-8 fails, try another encoding
                        content = response.content.decode('ISO-8859-1')

                    # Use codecs to handle Unicode escape sequences
                    cleaned_content = codecs.decode(content, 'unicode_escape')

                    if cleaned_content and response.headers['Content-Type'] == 'application/json':
                        data = json.loads(cleaned_content)
                        if 'data' in data:
                            df = pd.json_normalize(data['data'])
                            data_frames.append(df)
                    else:
                        print(f"The response is empty or not in the expected JSON format for URL: {url}")
                        break

        except requests.exceptions.RequestException as e:
            print(f"An error occurred while making the request to {url}: {e}")
    if data_frames:
        combined_dataframe = pd.concat(data_frames, ignore_index=True)
        combined_dataframe.to_sql('data_mart', conn, if_exists='replace', index=False)  # Use 'replace' or 'append'
    else:
        print("No data to concatenate.")
 