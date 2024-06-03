# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
# Start writing code
# df.show()
hotel_reviews = StructType([
    StructField("hotel_address", StringType(), True),
    StructField("additional_number_of_scoring", IntegerType(), True),
    StructField("review_date", TimestampType(), True),
    StructField("average_score", FloatType(), True),
    StructField("hotel_name", StringType(), True),
    StructField("reviewer_nationality", StringType(), True),
    StructField("negative_review", StringType(), True),
    StructField("review_total_negative_word_counts", IntegerType(), True),
    StructField("total_number_of_reviews", IntegerType(), True),
    StructField("positive_review", StringType(), True),
    StructField("review_total_positive_word_counts", IntegerType(), True),
    StructField("total_number_of_reviews_reviewer_has_given", IntegerType(), True),
    StructField("reviewer_score", FloatType(), True),
    StructField("tags", StringType(), True),
    StructField("days_since_review", StringType(), True),
    StructField("lat", FloatType(), True),
    StructField("lng", FloatType(), True)
])
df= hotel_reviews
ans= df.filter(lower('negative_review').contains('dirty')).filter(lower('hotel_address').contains('netherlands'))
ans.toPandas()
# final_ans= ans.filter(lower('hotel_address').contains('netherlands'))
# final_ans.toPandas()