sql = """
SELECT DATE_ID, MAKE_NAME, COUNT(DISTINCT LEAD_ID) AS UNIQUE_LEADS
, COUNT(DISTINCT PARTNER_ID) AS unique_partners
FROM DailySales
GROUP BY 1,2
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("DataFrameCreation").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("date_id", StringType(), True),
    StructField("make_name", StringType(), True),
    StructField("lead_id", IntegerType(), True),
    StructField("partner_id", IntegerType(), True)
])

# Create a list of rows to populate the DataFrame
data = [
    ("2020-12-08", "toyota", 0, 1),
    ("2020-12-08", "toyota", 1, 0),
    ("2020-12-08", "toyota", 1, 2),
    ("2020-12-07", "toyota", 0, 2),
    ("2020-12-07", "toyota", 0, 1),
    ("2020-12-08", "honda", 1, 2),
    ("2020-12-08", "honda", 2, 1),
    ("2020-12-07", "honda", 0, 1),
    ("2020-12-07", "honda", 1, 2),
    ("2020-12-07", "honda", 2, 1)
]

# Create the DataFrame from the data and schema
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.display()