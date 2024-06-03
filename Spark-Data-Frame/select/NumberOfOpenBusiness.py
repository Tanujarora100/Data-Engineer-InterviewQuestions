from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
# Start writing code
yelp_business=StructType(
    [
        StructField('business_id', StringType(), True),
        StructField('name', StringType(), True),
        StructField('address', StringType(), True),
        StructField('city', StringType(), True),
        StructField('state', StringType(), True),
        StructField('postal_code', StringType(), True),
        StructField('latitude', DoubleType(), True),
        StructField('longitude', DoubleType(), True),
        StructField('stars', DoubleType(), True),
        StructField('review_count', IntegerType(), True),
        StructField('is_open', IntegerType(), True),
        StructField('attributes', StringType(), True),
        StructField('categories', StringType(), True),
        StructField('hours', StringType(), True),
        StructField('is_open', IntegerType(), True),
        StructField('attributes', StringType(), True),
        StructField('categories', StringType(), True),
    ]
)
df= yelp_business
df_final= df.filter(col('is_open')==1).agg(count(col('business_id')).alias('total'))
df_final.toPandas()