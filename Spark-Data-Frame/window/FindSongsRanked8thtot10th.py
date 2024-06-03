# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# Start writing code
spotify_worldwide_daily_song_ranking = StructType([
    StructField('position', IntegerType(), True),
    StructField('track_name', StringType(), True),
    StructField('artist_name', StringType(), True),
    StructField('streams', IntegerType(), True),
    StructField('url', StringType(), True),
    StructField('date', StringType(), True),
    StructField('region', StringType(), True),
    StructField('position', IntegerType(), True),
    StructField('total_weekly_streams', IntegerType(), True),
    StructField('daily_rank', IntegerType(), True),
    StructField('total_daily_streams', IntegerType(), True),
    StructField('week', IntegerType(), True),
    StructField('year', IntegerType(), True),
    StructField('month', IntegerType(), True),
    StructField('day', IntegerType(), True),
    StructField('hour', IntegerType(), True),
])

# To validate your solution, convert your final pySpark df to a pandas df
spotify_worldwide_daily_song_ranking.filter(col('position').between('8', '10'))\
    .select('trackname', 'position')\
    .orderBy('position', ascending=True)\
    .toPandas()
