import psycopg2

conn = psycopg2.connect(
    "host=127.0.0.1 dbname=udacity user=udacity password=pass"
)

cur = conn.cursor()
conn.set_session(autocommit=True)

cur.execute(
    "CREATE TABLE IF NOT EXISTS test (col int, col2 int);"
)

cur.execute(
    "SELECT * FROM test;"
)
print('cur.fetchall():', cur.fetchall())
cur.close()
conn.close()
