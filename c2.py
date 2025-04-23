import psycopg2

conn = psycopg2.connect(
    dbname="nosql_db",
    user="postgres",
    password="new_password",
    host="localhost",
    port="5432"
)
cur = conn.cursor()
cur.execute("SELECT * FROM grades LIMIT 5;")
rows = cur.fetchall()
for row in rows:
    print(row)
cur.close()
conn.close()
