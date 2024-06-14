from pyspark.sql.functions import col, avg
from pyspark.sql.types import *
airbnb_search_details=StructType([
    StructField("city", StringType(), True),
    StructField("property_type", StringType(), True),
    StructField("bedrooms", IntegerType(), True),
    StructField("bathrooms", IntegerType(), True),
    StructField("price", IntegerType(), True),
    StructField("n_reviews", IntegerType(), True),
    StructField("n_guests", IntegerType(), True),
    StructField("n_beds", IntegerType(), True),
    StructField("n_baths", IntegerType(), True),
    StructField("n_rooms", IntegerType(), True),
    StructField("n_reviews", IntegerType(), True),
    StructField("n_guests", IntegerType(), True),
    StructField("n_beds", IntegerType(), True),
    StructField("n_baths", IntegerType(), True),
    StructField("n_rooms", IntegerType(), True),
])
airbnb_search_details=airbnb_search_details.groupBy('city','property_type')\
.agg(avg(col('bedrooms')).alias('average_bedrooms')
,avg(col('bathrooms')).alias('average_bathrooms'))

airbnb_search_details.toPandas()
