# Import your libraries
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Start writing code

forbes_global_2010_2014
df = forbes_global_2010_2014
window_spec = Window.orderBy(desc("profits"))

df = df.withColumn("rank", rank().over(window_spec)).filter(col("rank") <= 3)
final_df = df.select("company", "profits").toPandas()
