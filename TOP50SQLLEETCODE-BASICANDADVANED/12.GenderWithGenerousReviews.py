import pyspark.sql.functions as f
from pyspark.sql.types import *

sql_query = """
select g.gender, avg(r.review_score) as review_score
from airbnb_reviews as r
join airbnb_guests as g
on g.guest_id=r.from_user
where r.from_type='guest'
group by g.gender
order by avg(r.review_score) desc
limit 1

"""

# Start writing code
airbnb_reviews = StructType(
    [
        StructField("review_score", IntegerType(), True),
        StructField("from_user", IntegerType(), True),
        StructField("from_type", StringType(), True),
    ]
)

airbnb_guests = StructType(
    [
        StructField("guest_id", IntegerType(), True),
        StructField("gender", StringType(), True),
    ]
)

joined_df = airbnb_reviews.join(
    airbnb_guests, airbnb_reviews.from_user == airbnb_guests.guest_id, how="inner"
)
filtered_df = joined_df.filter(joined_df.from_type == "guest")

final_df = filtered_df.groupBy("gender").agg(
    f.avg("review_score").alias("average_score")
)
final_df = final_df.orderBy(f.desc("average_score")).limit(1)

# Show the results
final_df.toPandas()
