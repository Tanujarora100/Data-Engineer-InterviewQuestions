# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
yelp_reviews = StructType([
    StructField("review_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("business_id", StringType(), True),
    StructField("stars", IntegerType(), True),
    StructField("date", TimestampType(), True),
    StructField("text", StringType(), True),
    StructField("useful", IntegerType(), True),
    StructField("funny", IntegerType(), True),
    StructField("cool", IntegerType(), True),
    StructField("review_count", IntegerType(), True),
    StructField("average_stars", FloatType(), True),
    StructField("compliment_hot", IntegerType(), True),
    StructField("compliment_more", IntegerType(), True),
    StructField("compliment_profile", IntegerType(), True),
    StructField("compliment_cute", IntegerType(), True),
])
df = yelp_reviews
df = df.groupBy("stars").agg(count(col("review_id")).alias("total_review"))
final_df = df.orderBy("stars")
final_df.dropna().toPandas()
