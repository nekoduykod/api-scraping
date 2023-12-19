from connect_api import APIConnector
from files_utils import create_parquet, parquet_to_csv, write_to_database, previous_date_str

orders_params = {"date": previous_date_str}

api_connector = APIConnector()
response = api_connector.connect_api('orders', orders_params)

create_parquet(response, filename_prefix="orders")

parquet_to_csv(filename_prefix="orders")

write_to_database(filename_prefix="orders")