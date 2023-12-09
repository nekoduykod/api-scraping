import json
import requests
import psycopg2
from datetime import date, timedelta
import os

# db_params = os.getenv("DB_URL")

db_params = {
    'dbname': 'api_db',
    'user': 'postgres',
    'password': '123',
    'host': 'localhost',
    'port': '5432'
}

current_date = date.today()
previous_dates = current_date - timedelta(days=2)

current_date_str = current_date.strftime("%Y-%m-%d")
previous_dates_str = previous_dates.strftime("%Y-%m-%d")

url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/installs"

installs_params = {"date": previous_dates_str}

headers = {"Authorization": "***_0cRdtmCdC8EEZzpal1uUWdkR0TXgGkY0SXfehCy-4rUZh81Hr3PaZckxyJp3VIcgBzk8qGEpZRMD8_KBJukbtVYkaobYX7jMv4f2TA0kbXkCADTM2yCJw=="}

conn = psycopg2.connect(**db_params)

try:
    with conn.cursor() as cursor:
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS installs_table (
                install_time TIMESTAMP,
                marketing_id VARCHAR(255),
                channel VARCHAR(255),
                medium VARCHAR(255),
                campaign VARCHAR(255),
                keyword VARCHAR(255),
                ad_content VARCHAR(255),
                ad_group VARCHAR(255),
                landing_page VARCHAR(255),
                sex VARCHAR(255),
                alpha_2 VARCHAR(255),
                alpha_3 VARCHAR(255),
                flag VARCHAR(255),
                name VARCHAR(255),
                numeric VARCHAR(255),
                official_name VARCHAR(255)
            );
        '''
        cursor.execute(create_table_query)

        response = requests.get(url, params=installs_params, headers=headers)
        json_data = response.text.strip("}]'}").strip("{'[{") 
        json_data = response.text.replace("},{", ",")
        json_data = json.loads(json_data)
        print(json_data)

        json_objects = json_data

        for json_object in json_objects:
            if not json_object.endswith('}'):
                json_object += '}'
            if not json_object.startswith('{'):
                json_object = '{' + json_object
            record = json.loads(json_object)
 
            if record['flag'] is not None:
                record['flag'] = record['flag'].replace('\\', ' ') # will think later
            record['landing_page'] = record['landing_page'].strip("\\").strip("/")  # "landing_page":"\\/sale"

            insert_query = '''
                INSERT INTO installs_table VALUES (
                    %(install_time)s, %(marketing_id)s, %(channel)s, %(medium)s,
                    %(campaign)s, %(keyword)s, %(ad_content)s, %(ad_group)s,
                    %(landing_page)s, %(sex)s, %(alpha_2)s, %(alpha_3)s,
                    %(flag)s, %(name)s, %(numeric)s, %(official_name)s
                );
            '''
            cursor.execute(insert_query, record)
    conn.commit()
finally:
    conn.close()