import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# Reading the config file to source the AWS credentials
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Creates a SparkSession which will be one of the
    inputs of other functions in this code."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Creates dimensional tables- 'songs_table' and 'artists_table' from songs data log.
    First the song data log is fed into a spark df. Then tables are created based on columns required for star schema.
    Duplicate rows are removed using distinct.
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").parquet(output_data)

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data)


def process_log_data(spark, input_data, output_data):
    """Creates facts and dimensional tables. Songplays_table (facts table) and 'users_table' (dimensoinal) and 'time_table' (dimensional) from users data log and song data log.
    First the song data log is fed into a spark df. Then tables are created based on columns required for star schema.
    Duplicate rows are removed using distinct.
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter('page == "NextSong"')

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.fromtimestamp(x/1000), types.TimestampType())
    df = df.withColumn("start_time",get_timestamp("ts"))
    

    # extract columns to create time table
    time_table = df.select("start_time", hour("start_time").alias("hour"), dayofmonth("start_time").alias("day"), weekofyear("start_time").alias("week"), month("start_time").alias("month"), year("start_time").alias("year"), date_format("start_time","E").alias("weekday")).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").parquet(output_data)

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*.json")
    
    #Creating temp views for sql commands
    song_df.createOrReplaceTempView("songdataview")
    df.createOrReplaceTempView("logdataview")
    songplays_table = spark.sql("""
    select distinct start_time, userId as user_id, level, song_id, artist_id, sessionId as session_id, location, userAgent as user_agent from songdataview inner join logdataview on songdataview.title = logdataview.song and songdataview.artist_name = logdataview.artist
    """)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays_table.withColumn("songplay_id",monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").parquet(output_data)


def main():
    """Runs the above three functions. Making sure that the 'create_spark_funtion' is run first."""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://aws-logs-323643453072-us-west-2/atestfolder/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()