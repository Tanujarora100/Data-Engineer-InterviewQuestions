# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

billboard_top_100_year_end = StructType([
    StructField('year', IntegerType(), True),
    StructField('year_rank', IntegerType(), True),
    StructField('group_name', StringType(), True),
    StructField('artist', StringType(), True),
    StructField('song_name', StringType(), True),
    StructField('id', IntegerType(), True)
])
df= billboard_top_100_year_end
df= df.select('year','year_rank','group_name','song_name').distinct().filter(col('year')=='2010')
ranked_df= df.orderBy('year_rank').limit(10)
# ranked_df.show()
ranked_df.drop('year').toPandas()
# To validate your solution, convert your final pySpark df to a pandas df
# billboard_top_100_year_end.toPandas()