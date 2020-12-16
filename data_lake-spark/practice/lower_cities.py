from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession \
            .builder \
            .appName('lower_cities') \
            .getOrCreate()

    log_of_songs = [
        "Despacito",
        "Nice for what",
        "No tears left to cry",
        "Despacito",
        "Havana",
        "In my feelings",
        "Nice for what",
        "despacito",
        "All the stars"
    ]

    distributed_song_logs = spark.SparkContext.parallelize(log_of_songs)

    print('**********')
    print(distributed_song_logs.map(lambda song: song.lower()).collect())

    spark.close
