# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Restaurant Database").getOrCreate()
employee = StructType(
    [
        StructField("employee_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("sex", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("marital_status", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("date_of_joining", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postal_code", IntegerType(), True),
        StructField("phone_number", StringType(), True),
    ]
)
q1 = employee.groupBy("department").agg(
    sum(when(employee.sex == "F", 1).otherwise(0)).alias("female_counts"),
    count(when(employee.sex == "M", 1).otherwise(0)).alias("total_males"),
    sum(when(employee.sex == "F", 1).otherwise(0)).alias("total_female_salary"),
    sum(when(employee.sex == "M", 1).otherwise(0)).alias("total_male_salary"),
)
q1.toPandas()
sql_query = """
with female as(
    select
        department,
        count(distinct id) as total_females,
        sum(salary) as total_salary
    from
        employee
    where
        sex = 'F'
    group by
        1
),
male as(
    select
        department,
        count(distinct id) as total_males,
        sum(salary) as total_salary
    from
        employee
    where
        sex = 'M'
    group by
        1
)
select
    f.department,
    f.total_females,
    f.total_salary,
    m.total_males,
    m.total_salary
from
    female as f
    inner join male as m on f.department = m.department -- department | sex | #of emps | total salary
select
    department,
    COUNT(CASE WHEN sex = 'F' THEN id END) AS female_counts,
    SUM(CASE WHEN sex = 'F' THEN salary END) AS fem_sal,
    COUNT(CASE WHEN sex = 'M' THEN id END) AS male_counts,
    SUM(CASE WHEN sex = 'M' THEN salary END) AS mal_sal
from
    employee
GROUP BY
    1
"""
spark = spark.sql(sql_query)
