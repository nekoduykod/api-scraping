from api_connection import APIConnector
from csv_utils import create_csv, previous_date_str

orders_params = {"date": previous_date_str}

api_connector = APIConnector()
response = api_connector.connect_api('orders', orders_params)

create_csv(response, "orders", format="parquet")
# need to do here and csv_utils.py parquet logic
create_csv(____, "orders", to_database=True)