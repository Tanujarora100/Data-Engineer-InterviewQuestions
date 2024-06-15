from pyspark.sql.types import *
from pyspark.sql.functions import *

sql_query = """
SELECT profession as department,
SUM(CASE WHEN birth_month=1 THEN 1 ELSE 0 END) AS MONTH_1,
SUM(CASE WHEN birth_month=2 THEN 1 ELSE 0 END) AS MONTH_2,
SUM(CASE WHEN birth_month=3 THEN 1 ELSE 0 END) AS MONTH_3,
SUM(CASE WHEN birth_month=4 THEN 1 ELSE 0 END) AS MONTH_4,
SUM(CASE WHEN birth_month=5 THEN 1 ELSE 0 END) AS MONTH_5,
SUM(CASE WHEN birth_month=6 THEN 1 ELSE 0 END) AS MONTH_6,
SUM(CASE WHEN birth_month=7 THEN 1 ELSE 0 END) AS MONTH_7,
SUM(CASE WHEN birth_month=8 THEN 1 ELSE 0 END) AS MONTH_8,
SUM(CASE WHEN birth_month=9 THEN 1 ELSE 0 END) AS MONTH_9,
SUM(CASE WHEN birth_month=10 THEN 1 ELSE 0 END) AS MONTH_10,
SUM(CASE WHEN birth_month=11 THEN 1 ELSE 0 END) AS MONTH_11,
SUM(CASE WHEN birth_month=12 THEN 1 ELSE 0 END) AS MONTH_12
FROM employee_list
GROUP BY department
"""
employee_list = StructType(
    [
        StructField("profession", StringType(), True),
        StructField("MONTH_1", IntegerType(), True),
        StructField("MONTH_2", IntegerType(), True),
        StructField("MONTH_3", IntegerType(), True),
        StructField("MONTH_4", IntegerType(), True),
        StructField("MONTH_5", IntegerType(), True),
        StructField("MONTH_6", IntegerType(), True),
        StructField("MONTH_7", IntegerType(), True),
        StructField("MONTH_8", IntegerType(), True),
        StructField("MONTH_9", IntegerType(), True),
        StructField("MONTH_10", IntegerType(), True),
        StructField("MONTH_11", IntegerType(), True),
        StructField("MONTH_12", IntegerType(), True),
    ]
)

# Import your libraries
from pyspark.sql.functions import *

# Start writing code
df= employee_list.withColumn('year_born',year('birthday'))
df= df.groupBy('profession').pivot('birth_month').agg(countDistinct('employee_id'))
df.fillna(0).toPandas()