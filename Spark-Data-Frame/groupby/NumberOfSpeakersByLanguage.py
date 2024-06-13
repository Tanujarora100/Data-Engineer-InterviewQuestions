from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

# Create a Spark session
spark = SparkSession.builder.appName("Playbook Events Analysis").getOrCreate()
spark.sql(
    """
    SELECT 
        e.location,
        u.language,
        COUNT(DISTINCT u.user_id) AS total_count
    FROM 
        playbook_events AS e
    JOIN 
        playbook_users AS u
    ON 
        u.user_id = e.user_id
    GROUP BY 
        e.location,
        u.language
    ORDER BY 
        e.location ASC
"""
)
# Import your libraries


joined_df = playbook_events.join(playbook_users, on="user_id", how="inner")

# Group by location and language, and count distinct user_id
filtered_df = (
    joined_df.groupBy("location", "language")
    .agg(countDistinct("user_id").alias("total_count"))
    .orderBy(col("location").asc())
)

# Show the result
filtered_df.toPandas()

# playbook_events.toPandas()
