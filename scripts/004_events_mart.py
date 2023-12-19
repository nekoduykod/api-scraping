from connect_api import APIConnector
from files_utils import create_csv, write_to_database, previous_date_str

events_params = {"date": previous_date_str}
 
api_connector = APIConnector()
response = api_connector.connect_api('events', events_params)

data = api_connector.parse_events(response.text) 

create_csv(data=True, filename_prefix="events")

write_to_database(filename_prefix="events")