import configparser


# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artists"
time_table_drop           = "DROP TABLE IF EXISTS time"


# CREATE TABLES

staging_events_table_create= ("""
                              CREATE TABLE IF NOT EXISTS staging_events (
                                  artist VARCHAR,
                                  auth VARCHAR,
                                  firstName VARCHAR,
                                  gender CHAR,
                                  itemInSession SMALLINT,
                                  lastName VARCHAR,
                                  length FLOAT,
                                  level VARCHAR,
                                  location TEXT,
                                  method VARCHAR,
                                  page VARCHAR,
                                  registration FLOAT,
                                  sessionId INTEGER,
                                  song VARCHAR,
                                  status INTEGER,
                                  ts BIGINT,
                                  userAgent VARCHAR,
                                  userId INTEGER
                             ) ;
""")

staging_songs_table_create = ("""
                              CREATE TABLE IF NOT EXISTS staging_songs (
                                  num_songs INTEGER,
                                  artist_id VARCHAR,
                                  artist_latitude FLOAT,
                                  artist_longitude FLOAT,
                                  artist_location VARCHAR,
                                  artist_name VARCHAR,
                                  song_id VARCHAR,
                                  title VARCHAR,
                                  duration FLOAT,
                                  year INTEGER
                             ) ;
""")

songplay_table_create = ("""
                         CREATE TABLE IF NOT EXISTS songplays (
                            songplay_id INTEGER IDENTITY(1, 1) PRIMARY KEY,
                            start_time TIMESTAMP NOT NULL,
                            user_id INTEGER NOT NULL,
                            level VARCHAR NOT NULL,
                            song_id VARCHAR NOT NULL,
                            artist_id VARCHAR NOT NULL,
                            session_id INTEGER NOT NULL,
                            location VARCHAR,
                            user_agent VARCHAR )
                         DISTSTYLE key
                         DISTKEY (start_time)
                         SORTKEY (songplay_id) ;
""")

user_table_create = ("""
                     CREATE TABLE IF NOT EXISTS users (
                         user_id INTEGER PRIMARY KEY,
                         first_name VARCHAR,
                         last_name VARCHAR,
                         gender CHAR(1),
                         level VARCHAR )
                     DISTSTYLE all
                     SORTKEY (user_id);
""")

song_table_create = ("""
                     CREATE TABLE IF NOT EXISTS songs (
                         song_id VARCHAR PRIMARY KEY,
                         title VARCHAR NOT NULL,
                         artist_id VARCHAR NOT NULL,
                         year SMALLINT NOT NULL,
                         duration FLOAT NOT NULL)
                     SORTKEY (song_id);
""")

artist_table_create = ("""
                       CREATE TABLE IF NOT EXISTS artists (
                           artist_id VARCHAR PRIMARY KEY,
                           name VARCHAR,
                           location VARCHAR,
                           latitude FLOAT,
                           longitude FLOAT )
                       SORTKEY (artist_id);
""")

time_table_create = ("""
                     CREATE TABLE IF NOT EXISTS time (
                         start_time TIMESTAMP PRIMARY KEY,
                         hour INTEGER,
                         day INTEGER,
                         week INTEGER,
                         month INTEGER,
                         year INTEGER,
                         weekday VARCHAR )
                     DISTSTYLE key
                     DISTKEY (start_time)
                     SORTKEY (start_time);
""")


# STAGING TABLES

staging_events_copy = ("""
                       COPY staging_events
                       FROM {}
                       CREDENTIALS 'aws_iam_role={}'
                       FORMAT AS json {} ;
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
                      COPY staging_songs
                      FROM {}
                      CREDENTIALS 'aws_iam_role={}'
                      FORMAT AS json 'auto' ;
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# INSERT TO FINAL TABLES

songplay_table_insert = ("""
                         INSERT INTO songplays (
                            start_time,
                            user_id,
                            level,
                            song_id,
                            artist_id,
                            session_id,
                            location,
                            user_agent )
                         SELECT
                            DISTINCT TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' AS start_time,
                            e.userId AS user_id,
                            e.level,
                            s.song_id,
                            s.artist_id,
                            e.sessionId,
                            s.artist_location,
                            e.userAgent
                         FROM staging_events e
                         JOIN staging_songs s
                         ON e.song = s.title AND e.artist = s.artist_name AND e.page = 'NextSong' ;
""")

user_table_insert = ("""
                     INSERT INTO users (
                        user_id,
                        first_name,
                        last_name,
                        gender,
                        level )
                     SELECT
                        DISTINCT userId AS user_id,
                        firstName,
                        lastName,
                        gender,
                        level
                     FROM staging_events
                     WHERE userId IS NOT NULL AND page = 'NextSong' ;
""")

song_table_insert = ("""
                     INSERT INTO songs (
                        song_id,
                        title,
                        artist_id,
                        year,
                        duration )
                     SELECT
                        DISTINCT song_id,
                        title,
                        artist_id,
                        year,
                        duration
                     FROM staging_songs
                     WHERE song_id IS NOT NULL ;

""")

artist_table_insert = ("""
                       INSERT INTO artists (
                           artist_id,
                           name,
                           location,
                           latitude,
                           longitude )
                       SELECT
                           DISTINCT artist_id,
                           artist_name,
                           artist_location,
                           artist_latitude,
                           artist_longitude
                       FROM staging_songs
                       WHERE artist_id IS NOT NULL ;
""")

time_table_insert = ("""
                     INSERT INTO time (
                         start_time,
                         hour,
                         day,
                         week,
                         month,
                         year,
                         weekday )
                     SELECT
                         DISTINCT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time,
                         EXTRACT(hour from start_time) AS hour,
                         EXTRACT(day from start_time) AS day,
                         EXTRACT(week from start_time) AS week,
                         EXTRACT(month from start_time) AS month,
                         EXTRACT(year from start_time) AS year,
                         TO_CHAR(start_time, 'Day') AS weekday
                     FROM staging_events
                     WHERE page = 'NextSong' ;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
