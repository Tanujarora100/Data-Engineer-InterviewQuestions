from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
oscar_nominees = StructType([
    StructField('nominee', StringType(), True),
    StructField('category', StringType(), True),
    StructField('year', IntegerType(), True),
    StructField('winner', StringType(), True),
    StructField('id', IntegerType(), True),
    StructField('year_rank', IntegerType(), True),
    StructField('group_name', StringType(), True),
    StructField('artist', StringType(), True),
    StructField('song_name', StringType(), True),
    StructField('id', IntegerType(), True),
    StructField('year_rank', IntegerType(), True),
    StructField('group_name', StringType(), True),
    StructField('artist', StringType(), True),
    StructField('song_name', StringType(), True),
    StructField('id', IntegerType(), True),
    StructField('year_rank', IntegerType(), True),
])
df = oscar_nominees

df_winners = df.where(col('winner') == 'Y')\
               .groupBy('nominee')\
               .agg(count('id').alias('total_wins'))

window_spec = Window.orderBy(col('total_wins').desc())
df_windowed = df_winners.withColumn('rank', dense_rank().over(window_spec))

df_final = df_windowed.where(col('rank') == 1)
ans = df_final.drop(col('rank')).toPandas()
# df_final.show()
