SQL_QUERY = """
SELECT reviewer_nationality, 
    SUM(CASE WHEN LOWER(positive_review)<>'No Positive' THEN 1 ELSE 0 END ) AS N_REVIEWS
    FROM hotel_reviews
    GROUP BY reviewer_nationality
    HAVING N_REVIEWS > 0
    ORDER BY N_REVIEWS DESC
    
    
"""
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

hotel_reviews = StructType(
        
)
positive = hotel_reviews.filter(hotel_reviews["positive_review"] != "No Positive")
result = (
    positive.groupby("reviewer_nationality")
    .agg(F.count("positive_review").alias("n_positive_reviews"))
    .orderBy(F.desc("n_positive_reviews"))
)
result.toPandas()
