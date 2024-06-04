# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Restaurant Database").getOrCreate()


schema = StructType(
    [
        StructField("business_id", IntegerType()),
        StructField("business_name", StringType()),
        StructField("business_address", StringType()),
        StructField("business_city", StringType()),
        StructField("business_state", StringType()),
        StructField("business_postal_code", FloatType()),
        StructField("business_latitude", FloatType()),
        StructField("business_longitude", FloatType()),
        StructField("business_location", StringType()),
        StructField("business_phone_number", FloatType()),
        StructField("inspection_id", StringType()),
        StructField("inspection_date", TimestampType()),
        StructField("inspection_score", FloatType()),
        StructField("inspection_type", StringType()),
        StructField("violation_id", StringType()),
        StructField("violation_description", StringType()),
        StructField("risk_category", StringType()),
    ]
)

df = schema
filtered_df = df.filter(col("business_phone_number").isNotNull())
final_df = filtered_df.select("business_name").dropDuplicates()

final_df.toPandas()
