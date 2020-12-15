import cassandra
from cassandra.cluster import Cluster

try:
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS udacity
        WITH REPLICATION =
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
        """
    )
    session.set_keyspace('udacity')

    query = "CREATE TABLE IF NOT EXISTS music_library"
    query = query + "(year int, artist_name text, album_name text, PRIMARY KEY (year, artist_name))"
    session.execute(query)
    query = "INSERt INTO music_library (year, artist_name, album_name)"
    query = query + " VALUES (%s, %s, %s)"
    session.execute(query, (1970, "The Beatles", "Let It Be"))
    session.execute(query, (1965, "The Beatles", "Rubber Soul"))

    query = "SELECT * FROM music_library"
    rows = session.execute(query)
    for row in rows:
        print('all: ', row.year, row.album_name, row.artist_name)

    query = "select * from music_library WHERE YEAR=1970"
    rows = session.execute(query)
    for row in rows:
        print('where: ', row.year, row.album_name, row.artist_name)

    query = "drop table music_library"
    rows = session.execute(query)

    session.shutdown()
    cluster.shutdown()
except Exception as e:
    print(e)
