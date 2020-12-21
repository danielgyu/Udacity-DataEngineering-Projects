import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """create spark session for data processing"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes the song data and extracts necessary columns for two tables to be created, songs and artists.
    Then, each source is written to a S3 bucket in the parquet format.
        
    Params
    ------
    spark : pyspark session
        SparkSession object
    input_data : str
        prefix file path for the original data
    output_data : str
        prefix file path for the processed data
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    # song_id, title, artist_id, year, duration
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    song_path = output_data + "songs/"
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(song_path)

    # extract columns to create artists table
    # artist_id, name, location, lattitude, longitude
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artist_path = output_data + "artists/"
    artists_table.write.mode("overwrite").parquet(artist_path)


def process_log_data(spark, input_data, output_data):
    """
    Processes the log data and extracts necessary columns for two tables to be created, songs and artists.
    Then, each source is written to a S3 bucket in the parquet format.
    
    Params
    ------
    spark : pyspark session
        SparkSession object
    input_data : str
        prefix file path for the original data
    output_data : str
        prefix file path for the processed data
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*"

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()
    
    # filter by actions for song plays
    df = df.filter("page == 'NextSong'")

    # extract columns for users table    
    # user_id, first_name, last_name, gender, level
    users_table = df.selectExpr(
                            "userId as user_id", 
                            "firstName as first_name",
                            "lastName as last_name",
                            "gender",
                            "level") \
                    .dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_path = output_data + "users/"
    users_table.write.mode("overwrite").parquet(users_path)

    # create timestamp column from original timestamp column
    # get_timestamp = udf(lambda ts: int(ts/1000), IntegerType())
    # df = df.withColumn("timestamp", get_timestamp("ts"))
    
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
            dayofmonth(start_time) as day,
            weekofyear(start_time) as week,
            month(start_time) as month,
            year(start_time) as year,
            dayofweek(start_time) as weekday
        FROM
            log_data_view
        WHERE
            start_time IS NOT NULL
    """).dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_path = output_data + "time/"
    time_table.write.mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(time_path)

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*"
    song_df = spark.read.json(song_data).dropDuplicates()
    song_df.createOrReplaceTempView('song_data_view')

    # extract columns from joined song and log datasets to create songplays table 
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_table = spark.sql("""
        SELECT
            monotonically_increasing_id() AS songplay_id,
            l.start_time,
            l.userId AS user_id,
            l.level,
            s.song_id,
            s.artist_id,
            l.sessionId AS session_id,
            l.location,
            l.userAgent AS user_agent,
            year(l.start_time) as year,
            month(l.start_time) as month
        FROM
            log_data_view l
        JOIN
            song_data_view s ON (l.artist = s.artist_name)
    """)

    # write songplays table to parquet files partitioned by year and month
    songplay_path = output_data + "songplays/"
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(songplay_path)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-emr/sparkify/data-lake-project/"
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
