from api_connection import APIConnector
from csv_utils import create_csv, previous_date_str

events_params = {"date": previous_date_str}
 
api_connector = APIConnector()
response = api_connector.connect_api('events', events_params)

data = api_connector.parse_events(response.text) 

create_csv(data, "events", to_database=True)