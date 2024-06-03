from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, IntegerType,StringType,DateType
from pyspark.sql.window import Window
worker= StructType([
    StructField('worker_id', IntegerType(), True),
    StructField('first_name', StringType(), True),
    StructField('last_name', StringType(), True),
    StructField('worker_title', StringType(), True),
    StructField('salary', IntegerType(), True),
    StructField('joining_date', DateType(), True),
    StructField('department', StringType(), True)

])
worker= worker.withColumn('rnk',row_number().over(Window.orderBy(desc('worker_id'))))
worker=worker.filter(col('rnk')==1).drop('rnk')
# worker.show()
# To validate your solution, convert your final pySpark df to a pandas df
worker.toPandas()