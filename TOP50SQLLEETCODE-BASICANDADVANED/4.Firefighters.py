from pyspark.sql.functions import *
from pyspark.sql.types import *

sf_public_salaries = StructType(
    [
        StructField("employeename", StringType(), True),
        StructField("jobtitle", StringType(), True),
        StructField("basepay", FloatType(), True),
        StructField("overtimepay", FloatType(), True),
        StructField("otherpay", FloatType(), True),
        StructField("totalpay", FloatType(), True),
        StructField("benefits", FloatType(), True),
        StructField("totalpaybenefits", FloatType(), True),
        StructField("year", IntegerType(), True),
        StructField("yeartype", StringType(), True),
        StructField("supervisortitle", StringType(), True),
        StructField("department", StringType(), True),
        StructField("employeeid", IntegerType(), True),
        StructField("employeetype", StringType(), True),
        StructField("jobstatus", StringType(), True),
    ]
)
df = sf_public_salaries
df = sf_public_salaries.withColumn(
    "company",
    when(lower(col("jobtitle")).contains("fire"), "Firefighter")
    .when(lower(col("jobtitle")).contains("police"), "Police")
    .otherwise("Null"),
)

# df.show(100)
grouped_df = df.groupBy("company").agg(count("*").alias("total_count"))
grouped_df.filter(col("company") != "Null").toPandas()


sql_df = """
with cte as(
select case when(lower(fire) in lower(jobtitle) then "Firefighter" else "Null") 
    when(lower(jobtitle) in lower(jobtitle) then "Police" else "Null")
    end as Company
    )

    select company, count(*) as total_count
    from cte 
    group by 1
    having company <> 'Null'

"""
