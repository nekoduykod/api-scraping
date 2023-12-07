import sqlite3

with sqlite3.connect('data_marts.db') as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM orders_table LIMIT 100")
    rows = cursor.fetchall()

    for row in rows:
        print(row)