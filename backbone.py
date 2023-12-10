import requests
from datetime import date, timedelta
import csv
import json
import psycopg2

# db_params = {
#     'dbname': 'api_db',
#     'user': 'postgres',
#     'password': '123',
#     'host': 'localhost',
#     'port': '5432'
# }

current_date = date.today()
previous_dates = current_date - timedelta(days=2)
previous_dates_str = previous_dates.strftime("%Y-%m-%d")

url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/installs"
installs_params = {"date": previous_dates_str}

headers = {"Authorization": "***_***pTw3NhQ7ysIwYIn8EEZzpal1uUWdkR0TXgGkY0SXfehCy-4rUZh81Hr3PaZckxyJp3VIcgBzk8qGEpZRMD8_KBJukbtVYkaobYX7jMv4f2TA0kbXkCADTM2yCJw=="}

response = requests.get(url, params=installs_params, headers=headers)
# print("Status Code:", response.status_code)
 
data = response.text.replace('\\', '').replace('}]"}', '}]').replace('{"count":648,"records":"', '').replace('/', '')
# data = response.text.replace('\\', '').replace("},{", ",").replace('}]"}', '').replace('{"count":648,"records":"[{', '').replace('/', '')
# content_type = response.headers.get('Content-Type', '')
print(data)






# conn = psycopg2.connect(**db_params)


# with open('installs.json', 'w') as f:
#    json.dump(data, f)
 


# if response.status_code == 200:
#     data = response.json()
#     csv_file_path = "01_installs_data.csv"
#     with open(csv_file_path, mode='w', newline='') as csv_file:
#         writer = csv.writer(csv_file)

#         # Write header
#         writer.writerow(data[0].keys())

#         # Write data
#         for row in data:
#             writer.writerow(row.values())

#     print(f"Data has been saved to {csv_file_path}")
# else:
#     print(f"Error: {response.status_code}")
#     print(response.text)




# print("Response Content:")
# print("Content Type:", response.headers.get('Content-Type'))
 
