import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, year, month, hour, monotonically_increasing_id
from pyspark.sql.types import IntegerType, LongType


config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Instantiates and returns a SparkSession Object.
    
    Returns:
        SparkSession: SparkSession object.   
    """
    
    try:
        spark = SparkSession.builder \
                        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
                        .getOrCreate()
        return spark
    except Exception as e:
        print(e)


def process_song_data(spark, input_data, output_data):
    """Processes song data to create song and artist tables.
    
    Loads data from song files in an AWS S3 bucket, extracts the relevant 
    song and artist fields, and writes them to the corresponding song and
    artist tables stored in an AWS S3 bucket.
    
    Args:
        spark (:obj:`SparkSession`): SparkSession object.
        input_data (:obj:`str`): string specifying the URL of the AWS S3 
            bucket that stores the song files.    
        output_data (:obj:`str`): string specifying the URL of the AWS S3 
            bucket that stores the created tables.
    """
    
    global song_df
    try:
        song_data = input_data + "/song_data/*/*/*"
        song_df = spark.read.format("json").load(song_data)        
        
        songs_table = song_df.select(["song_id","title","artist_id","year","duration"]) \
                             .filter(song_df.song_id != "null") \
                             .dropDuplicates()  
        songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data,'songs'))
    
    
        artists_table = song_df.selectExpr(["artist_id","artist_name as name", "artist_location as location", 
                                  "artist_latitude as latitude", "artist_longitude as longitude"]) \
                               .filter(song_df.artist_id != "null") \
                               .dropDuplicates()
        artists_table.write.parquet(os.path.join(output_data,'artists'))
    except Exception as e:
        print(e)


def process_log_data(spark, input_data, output_data):
    """Processes log and song data to create user, time and songplay tables.
    
    Loads data from event log files in an AWS S3 bucket, extracts the relevant 
    user and time fields, and writes them to the corresponding user and
    time tables stored in an AWS S3 bucket.
    Also creates and stores the songplay table in the S3 bucket, from the fields
    extracted from both the event log and song data.
    
    Args:
        spark (:obj:`SparkSession`): SparkSession object.
        input_data (:obj:`str`): string specifying the URL of the AWS S3 
            bucket that stores the song files.    
        output_data (:obj:`str`): string specifying the URL of the AWS S3 
            bucket that stores the created tables.
    """

    try:
        log_data = input_data + "/log_data/*"
        df = spark.read.format("json").load(log_data)
        df = df.filter(df.page == 'NextSong')
    
        users_table = df.selectExpr(["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]) \
                        .filter(df.userId != "null") \
                        .dropDuplicates()  
        users_table.write.parquet(os.path.join(output_data,'users'))

        
        get_timestamp = udf(lambda x : x // 1000, LongType())
        
        get_hour = udf(lambda x : datetime.fromtimestamp(x).hour, IntegerType())
        df = df.withColumn("hour", get_hour(get_timestamp("ts")))
    
        get_day = udf(lambda x : datetime.fromtimestamp(x).day, IntegerType())
        df = df.withColumn("day", get_day(get_timestamp("ts")))
    
        get_week = udf(lambda x : datetime.fromtimestamp(x).isocalendar()[1], IntegerType())
        df = df.withColumn("week", get_week(get_timestamp("ts")))
    
        get_month = udf(lambda x : datetime.fromtimestamp(x).month, IntegerType())
        df = df.withColumn("month", get_month(get_timestamp("ts")))
    
        get_year = udf(lambda x : datetime.fromtimestamp(x).year, IntegerType())
        df = df.withColumn("year", get_year(get_timestamp("ts")))
    
        get_weekday = udf(lambda x : datetime.fromtimestamp(x).isoweekday(), IntegerType())
        df = df.withColumn("weekday", get_weekday(get_timestamp("ts")))
    
        time_table = df.selectExpr(["ts as start_time", "hour", "day", "week", "month", "year", "weekday"]) \
                       .dropDuplicates()
        time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data,'time'))
    
   
        song_df.createOrReplaceTempView("songs_table")
        df.createOrReplaceTempView("logs_table")
        songplays_table = spark.sql("SELECT 1 as songplay_id, logs_table.ts as start_time, logs_table.userId as user_id, logs_table.level as level, \
                                     songs_table.song_id as song_id, songs_table.artist_id as artist_id, logs_table.sessionId as session_id, \
                                     logs_table.location as location, logs_table.userAgent as user_agent, logs_table.year as year, \
                                     logs_table.month as month \
                                     FROM songs_table RIGHT JOIN logs_table \
                                     ON songs_table.artist_name = logs_table.artist AND songs_table.title = logs_table.song") \
                               .dropDuplicates()
        songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
        songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data,'songplays'))
    except Exception as e:
        print(e)


def main():
    spark = create_spark_session()
    input_data = "s3://"    
    output_data = "s3://"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()