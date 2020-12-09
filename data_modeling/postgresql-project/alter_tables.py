import psycopg2

def connect_to_database():
    """
    기존 데이터베이스에 접근
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    return cur, conn

song_table_alter = ("""
    ALTER TABLE IF EXISTS songs
    ADD CONSTRAINT fk_song_songplay
    FOREIGN KEY (artist_id)
    REFERENCES artists (artist_id)
    ON DELETE CASCADE
""")

songplay_table_alter_time = ("""
    ALTER TABLE IF EXISTS songplays ADD
    CONSTRAINT fk_songplay_time
        FOREIGN KEY (ts) REFERENCES time (ts)
""")

songplay_table_alter_user = ("""
    ALTER TABLE IF EXISTS songplays ADD
    CONSTRAINT fk_songplay_user
        FOREIGN KEY (user_id) REFERENCES users (user_id)
""")

songplay_table_alter_song = ("""
    ALTER TABLE IF EXISTS songplays ADD
    CONSTRAINT fk_songplay_song
        FOREIGN KEY (song_id) REFERENCES songs (song_id)
""")

songplay_table_alter_artist = ("""
    ALTER TABLE IF EXISTS songplays ADD
    CONSTRAINT fk_songplay_artist
        FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
""")

def main():
    """
    데이터베이스에 접근 및 FK 추가
    """
    cur, conn = connect_to_database()
    cur.execute(song_table_alter)
    songplay_table_alter = [songplay_table_alter_time, songplay_table_alter_user, songplay_table_alter_song, songplay_table_alter_artist]
    for alter in songplay_table_alter:
        cur.execute(alter)
    conn.commit()
    conn.close()
    print(f'alter complete')

if __name__ == '__main__':
    main()


