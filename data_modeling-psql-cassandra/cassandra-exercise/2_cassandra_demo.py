import cassandra
from cassandra.cluster import Cluster

try:
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS udacity
                    WITH REPLICATION = {
                    'class' : 'SimpleStrategy',
                    'replication_factor' : 1 }
                    """)
    session.set_keyspace('udacity')

    # create tables
    query = "CREATE TABLE IF NOT EXISTS music_library"
    query = query + "(year int, artist_name varchar, album_name varchar, PRIMARY KEY (year, artist_name))"
    session.execute(query)
    query1 = "CREATE TABLE IF NOT EXISTS artist_library"
    query1 = query1 + "(year int, artist_name varchar, album_name varchar, PRIMARY KEY (artist_name, year))"
    session.execute(query1)
    query2 = "CREATE TABLE IF NOT EXISTS album_library"
    query2 = query2 + "(year int, artist_name varchar, album_name varchar, PRIMARY KEY (album_name, artist_name))"
    session.execute(query2)

    # insert data
    query = "INSERT INTO music_library (year, artist_name, album_name)"
    query = query + " VALUES (%s, %s, %s)"

    query1 = "INSERT INTO artist_library (artist_name, year, album_name)"
    query1 = query1 + " VALUES (%s, %s, %s)"

    query2 = "INSERT INTO album_library (album_name, artist_name, year)"
    query2 = query2 + " VALUES (%s, %s, %s)"

    session.execute(query, (1970, "The Beatles", "Let it Be"))
    session.execute(query, (1965, "The Beatles", "Rubber Soul"))
    session.execute(query, (1965, "The Who", "My Generation"))
    session.execute(query, (1966, "The Monkees", "The Monkees"))
    session.execute(query, (1970, "The Carpenters", "Close To You"))
    session.execute(query1, ("The Beatles", 1970, "Let it Be"))
    session.execute(query1, ("The Beatles", 1965, "Rubber Soul"))
    session.execute(query1, ("The Who", 1965, "My Generation"))
    session.execute(query1, ("The Monkees", 1966, "The Monkees"))
    session.execute(query1, ("The Carpenters", 1970, "Close To You"))
    session.execute(query2, ("Let it Be", "The Beatles", 1970))
    session.execute(query2, ("Rubber Soul", "The Beatles", 1965))
    session.execute(query2, ("My Generation", "The Who", 1965))
    session.execute(query2, ("The Monkees", "The Monkees", 1966))
    session.execute(query2, ("Close To You", "The Carpenters", 1970))

    # query msuic_library table
    query = "select * from music_library WHERE year = 1970"
    rows = session.execute(query)
    for row in rows:
        print (row.year, row.artist_name, row.album_name)

    # query artist_library table
    query = "select * from artist_library WHERE artist_name = 'The Beatles'"
    rows = session.execute(query)
    for row in rows:
        print (row.artist_name, row.album_name, row.year)

    # query album_library table
    query = "select * from album_library WHERE album_name = 'Close To You'"
    rows = session.execute(query)
    for row in rows:
        print (row.artist_name, row.year, row.album_name)

    session.shutdown()
    cluster.shutdown()
except Exception as e:
    print(e)
