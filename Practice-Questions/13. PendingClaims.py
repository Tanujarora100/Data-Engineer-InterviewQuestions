from pyspark.sql.types import *
from pyspark.sql.functions import *
sql_query = """
SELECT COUNT(*) 
    AS PENDING_CLAIMS
FROM (SELECT * FROM cvs_claims 
    WHERE date_rejected IS NULL 
        AND date_accepted IS NULL) as subquery
    WHERE YEAR(DATE_SUBMITTED)=2021 
    AND 
        MONTH(DATE_SUBMITTED)=12
"""

cvs_claims = StructType([
    StructField("claim_id", IntegerType(), True),
    StructField("date_submitted", DateType(), True),
    StructField("date_accepted", DateType(), True),
    StructField("date_rejected", DateType(), True),
    StructField("claim_status", StringType(), True),
    StructField("claim_type", StringType(), True),
    StructField("claim_amount", IntegerType(), True),
    StructField("claim_description", StringType(), True),
    StructField("claim_category", StringType(), True),
    StructField("claim_subcategory", StringType(), True),
    StructField("claim_provider", StringType(), True),
    StructField("claim_provider_id", IntegerType(), True),
    StructField("claim_provider_name", StringType(), True),
    StructField("claim_provider_address", StringType(), True),
])

df = cvs_claims

df = (
    df.filter(
        (year(col("date_submitted")) == 2021) & (
            month(col("date_submitted")) == 12)
    )
    .filter((col("date_accepted").isNull()) & (col("date_rejected").isNull()))
    .agg(count("claim_id").alias("count_pending_claim"))
)

df.toPandas()
