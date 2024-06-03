# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

flight_schedule = StructField([

])
entertainment_catalog = StructField([

])
flight_movies_cross = flight_schedule.join(entertainment_catalog)

# flight_movies_cross.show()
filtered_movies = flight_movies_cross.filter(
    flight_movies_cross.duration <= flight_movies_cross.flight_duration).where(col('flight_id') == 101)
final_df = filtered_movies.select('flight_id', 'movie_id', col('duration').alias('movie_duration'))\
    .orderBy('movie_duration', ascending=True).toPandas()
