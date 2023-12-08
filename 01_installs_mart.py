import requests
from datetime import date, timedelta
import csv
import json
import psycopg2

db_params = {
    'dbname': 'api_db',
    'user': 'postgres',
    'password': '123',
    'host': 'localhost',
    'port': '5432'
}

current_date = date.today()
previous_dates = current_date - timedelta(days=2)
previous_dates_str = previous_dates.strftime("%Y-%m-%d")

url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/installs"
installs_params = {"date": previous_dates_str}

headers = {"Authorization": "***_0cRdtmCdCu9XFkFNs45dpTw3NhQ7ysIwYIn8EEZzpal1uUWdkR0TXgGkY0SXfehCy-4rUZh81Hr3PaZckxyJp3VIcgBzk8qGEpZRMD8_KBJukbtVYkaobYX7jMv4f2TA0kbXkCADTM2yCJw=="}

conn = psycopg2.connect(**db_params)
 
try:
    with conn.cursor() as cursor:
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS installs_mart (
                install_time VARCHAR(255),
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
        data = response.text.replace('\\', '').replace('}]"}', '}]').replace('{"count":648,"records":"', '').replace('/', '')
        # print(data[:1000])
        records = json.loads(data)
        for record in records:
            insert_query = '''
                INSERT INTO installs_mart VALUES (
                    %(install_time)s, %(marketing_id)s, %(channel)s, %(medium)s,
                    %(campaign)s, %(keyword)s, %(ad_content)s, %(ad_group)s,
                    %(landing_page)s, %(sex)s, %(alpha_2)s, %(alpha_3)s,
                    %(flag)s, %(name)s, %(numeric)s, %(official_name)s
                );
                '''
            cursor.execute(insert_query, record)
    conn.commit()
    print('Loaded successfully to PostgreSQL.')

    csv_filename = 'installs_data.csv'
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=records[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(records)
    print('Data written to CSV:', csv_filename)

finally:
    conn.close()
