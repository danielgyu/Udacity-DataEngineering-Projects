import psycopg2

try:
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=udacity user=udacity password=pass"
    )
    cur = conn.cursor()
    conn.set_session(autocommit=True)
    cur.execute(
        "CREATE TABLE IF NOT EXISTS music_library ( \
        album_name varchar, \
        artist_name varchar, \
        year int \
    );")
    cur.execute(
        "SELECT COUNT(*) FROM music_library"
    )
    print(cur.fetchall())
    cur.execute(
        "INSERT INTO music_library (album_name, artist_name, year) \
        VALUES (%s, %s, %s)", \
        ("Let It Be", "The Beatles", 1970))
    cur.execute(
        "INSERT INTO music_library (album_name, artist_name, year) \
        VALUES (%s, %s, %s)", \
        ("Let It Be", "The Beatles", 1970))
    cur.execute(
        "SELECT * FROM music_library;"
    )
    row = cur.fetchone()
    while row:
        print(row)
        row = cur.fetchone()
    cur.execute(
        "DROP TABLE music_library"
    )
    cur.close()
    conn.close()
except psycopg2.Error as e:
    print(e)
