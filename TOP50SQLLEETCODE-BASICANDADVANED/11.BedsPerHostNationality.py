import pyspark.sql.functions as f
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
airbnb_hosts = StructType(
    [
        StructField("host_id", IntegerType(), True),
        StructField("nationality", StringType(), True),
    ]
)
df = airbnb_apartments
joined_df = df.join(airbnb_hosts, on="host_id", how="inner")
final_df = joined_df.groupBy("nationality").agg(f.sum("n_beds").alias("total_beds"))
final_df = final_df.orderBy(f.desc("total_beds"))
final_df.toPandas()


sql_query = """
select h.nationality, sum(a.n_beds) as total_beds
from airbnb_apartments as a
inner join airbnb_hosts as h
on 
    a.host_id=h.host_id
group by h.nationality
order by sum(a.n_beds) desc
"""
