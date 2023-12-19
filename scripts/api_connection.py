import os
import requests
from dotenv import load_dotenv

class APIConnector:
    def __init__(self):
        load_dotenv()
        self.url = os.environ.get("API_URL")
        self.auth_header = os.environ.get("MY_AUTH_HEADER")

    def connect_api(self, endpoint, params):
        full_url = f"{self.url}/{endpoint}"
        headers = {"Authorization": self.auth_header}
        response = requests.get(full_url, params=params, headers=headers)
        return response
    
    def parse_installs(response):
        data = response.replace('\\', '').replace('}]"}', '}]').replace('/', '')
        prefix = '{"count":'       
        count_start = data.find(prefix)
        count_end = data.find(',"records":"') + len(',"records":"')  
        characters_to_strip = count_end - count_start
        data = data[characters_to_strip:]
        return data
    
    def parse_costs(response):
        data = response.replace('\t', ',').replace('/home', 'home').replace('/sale', 'sale')\
                        .replace('/signup', 'signup').replace('/new-arrivals', 'new-arrivals').replace('/products', 'products')
        return data
    
    def parse_events(response):
        data = response.replace('\\', '').replace('{"data":"', '').replace('}}]",', '}}]')
        next_page_index = data.find('"next_page"')
        data = data[:next_page_index]
        return data