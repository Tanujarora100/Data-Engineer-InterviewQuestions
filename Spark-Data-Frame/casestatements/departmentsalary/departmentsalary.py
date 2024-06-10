# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

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
