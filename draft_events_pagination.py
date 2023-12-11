import requests
from datetime import date, timedelta
from urllib.parse import quote

def parse_response(response):
    data = response.text.replace('\\', '').replace('{"data":"', '').replace('}}]"', '}}') 
    #                     .replace('"next_page"', '"}, "next_page"')
    # data = data + ']'
    
    # # Ensure the last object is enclosed in curly braces
    # last_object_index = data.rfind('}}')
    # if last_object_index != -1:
    #     data = data[:last_object_index + 2]  
    return data
  
current_date = date.today()
previous_dates = current_date - timedelta(days=3)
previous_dates_str = previous_dates.strftime("%Y-%m-%d")

url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/events"
headers = {"Authorization": "***"}

events_params = {"date": previous_dates_str}

while True:
    response = requests.get(url, params=events_params, headers=headers)
    try:
        data = response.json()
        print(data)
        next_page_token = data.get('next_page')
        if next_page_token:
            events_params["next_page"] = next_page_token
        else:
            print("No more pages available.")
            break

    except requests.exceptions.JSONDecodeError:
        print("Failed to decode JSON. Response might not be in valid JSON format.")
        break 