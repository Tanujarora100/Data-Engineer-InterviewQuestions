import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

airbnb_apartments = StructType(
    [
        StructField("host_id", IntegerType(), True),
        StructField("n_beds", IntegerType(), True),
        StructField("n_baths", IntegerType(), True),
        StructField("n_rooms", IntegerType(), True),
        StructField("n_guests", IntegerType(), True),
        StructField("price", IntegerType(), True),
        StructField("n_reviews", IntegerType(), True),
        StructField("last_review", DateType(), True),
        StructField("review_scores_rating", IntegerType(), True),
        StructField("review_scores_accuracy", IntegerType(), True),
        StructField("review_scores_cleanliness", IntegerType(), True),
        StructField("review_scores_checkin", IntegerType(), True),
        StructField("review_scores_communication", IntegerType(), True),
        StructField("review_scores_location", IntegerType()),
    ]
)
df = airbnb_apartments

df = df.groupBy("host_id").agg(F.sum("n_beds").alias("total_beds"))
window_spec = Window.orderBy(F.desc("total_beds"))
final_df = df.withColumn("rank", F.dense_rank().over(window_spec))
final_df.toPandas()

sql_query = """
select host_id, 
    sum(n_beds) as number_of_beds, 
    dense_rank() over(order by sum(n_beds) desc) as rank
from airbnb_apartments
group by 1
order by rank asc
"""
