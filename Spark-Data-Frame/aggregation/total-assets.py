# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# Start writing code
forbes_global_2010_2014
df=forbes_global_2010_2014
df=df.filter(col('sector')=='Energy').agg(sum(col('assets')))
df.show()
df.toPandas()