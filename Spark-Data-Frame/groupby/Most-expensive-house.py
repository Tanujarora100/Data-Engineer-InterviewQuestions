# Import your libraries
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

# Start writing code
zillow_transactions=StructType([
    StructField('id',IntegerType,False),
    StructField('state',StringType,True),
    StructField('city',StringType,True),
    StructField('street_address',StringType,True),
    StructField('mkt_price',IntegerType,True)
])
df= zillow_transactions
df= df.groupBy('city').agg(mean(col('mkt_price')).alias('avg_price_city'))
mean_price= df.select(mean(col('avg_price_city'))).first()[0]
df= df.filter(col('avg_price_city')>mean_price).select(col('city')).toPandas()
