import requests
import pandas as pd
from datetime import date, timedelta 


current_date = date.today()
previous_date = current_date - timedelta(days=1)
previous_date_str = previous_date.strftime("%Y-%m-%d")

url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/"

installs_url = url + "installs"
costs_url = url + "costs"
orders_url = url + "orders"
events_url = url + "events"

installs_params = {"date": previous_date_str}
costs_params = {"date": previous_date_str, "dimensions": "location,channel,medium,campaign,keyword,ad_content,ad_group,landing_page"}
orders_params = {"date": previous_date_str}
events_params = {"date": previous_date_str}

headers = {"Authorization": " *** "}

