import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Inserts data into the songs table and artists table from song data files.

    Params
    ------
    cur : psycopg2.extensions.cursor
        A cursor object of the psycopg2 library
    filepath : str
        Path to the file that contains our data
    """
    df = pd.read_json(filepath, typ='series')

    columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = df[[*columns]]
    cur.execute(song_table_insert, song_data)

    columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_data = df[[*columns]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    First Inserts data into the time table and users table from log data files.
    Then joins log data with data from songs table to populate songplays table.

    Params
    ------
    cur : psycopg2.extensions.cursor
        A cursor object of the psycopg2 library
    filepath : str
        Path to the file that contains our data
    """
    df = pd.read_json(filepath, lines=True)

    df = df[df['page'] == 'NextSong'].astype({'ts':'datetime64[ms]'})

    t = pd.Series(df['ts'], index=df.index)

    time_data = [[d, d.hour, d.day, d.week, d.month, d.year, d.weekday()] for d in t]
    column_labels= ['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(data=time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df[[*columns]]
    user_df = user_df[user_df.firstName.notnull()]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Retrieves all files(.json) under the input filepath.
    Then applies input function to the corresponding files.

    Params
    ------
    cur : psycopg2.extensions.cursor
        A cursor object of the psycopg2 library
    filepath : str
        Path to the file that contains our data
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
    print('{} files processed.'.format(num_files))

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
