import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description
    
    Params
    ------
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*"

    # read song data file
    df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    # song_id, title, artist_id, year, duration
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    song_path = output_data + "songs/"
    songs_table = df.write.mode("overwrite").partitionBy("year", "artist").parquet(song_path)

    # extract columns to create artists table
    # artist_id, name, location, lattitude, longitude
    artists_table = df.select("artist_id", "name", "location", "latitude", "longitude").dropDuplicates()

    # write artists table to parquet files
    artist_path = output_data + "artists/"
    artists_table = df.write.mode("overwrite").parquet(artist_path)


def process_log_data(spark, input_data, output_data):
    """
    Description
    
    Params
    ------
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*"

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()

    # filter by actions for song plays
    df = df.filter("page == 'NextSong'")

    # extract columns for users table    
    # user_id, first_name, last_name, gender, level
    artists_table = df.select("user_id", "first_name", "last_name", "gender", "level").dropDuplicates()

    # write users table to parquet files
    artist_path = output_data + "users/"
    artists_table.write.mode("overwrite").parquet(artist_path)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: ts/1000, TimestampType())
    df = df.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts/1000), TimestampType())
    df = df.withColumn("start_time", get_datetime("ts"))
    
    # create view table for sql query
    df.createOrReplaceTempView('log_data_view')
    
    # extract columns to create time table
    # start_time, hour, day, week, month, year, weekday
    time_table = spark.sql("""
        SELECT
            start_time,
            hour(start_time) as hour,
            day(start_time) as day,
            week(start_time) as week,
            month(start_time) as month,
            year(start_time) as year,
            dayofweek(start_time) as weekday
        FROM
            log_data_view
        WHERE
            start_time IS NOT NULL
    """)
    
    # write time table to parquet files partitioned by year and month
    time_path = output_data + "time/"
    time_table.write.mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(time_path)

    # read in song data to use for songplays table
    song_path = output_data + "songs/"
    song_df = spark.read \
                .format("parquet") \
                .load(path)

    # extract columns from joined song and log datasets to create songplays table 
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_table = df.select("df.ts", "df.userId", "df.level", "song_df.song_id",
                                "song_df.artist_id", "df.sessionId", "df.location", "df.userAgent") \
                        .join(song_df, (df.artist == song_df.artist_name), "inner")

    # write songplays table to parquet files partitioned by year and month
    songplay_path = output_data + "songplays/"
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(songplay_path)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-emr/sparkify/data-lake-p"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
