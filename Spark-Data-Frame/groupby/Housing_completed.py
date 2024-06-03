# Start writing code
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
housing_units_completed_us= StructType([
    StructField('year', IntegerType(), True),
    StructField('south', IntegerType(), True),
    StructField('west', IntegerType(), True),
    StructField('midwest', IntegerType(), True),
    StructField('northeast', IntegerType(), True)
])
df= housing_units_completed_us
df=df.groupBy(col('year')).agg(sum(col('south')+col('west')+col('midwest')+col('northeast')).alias('total'))
df.orderBy('year').show()