# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
iris= StructType([
    StructField("sepal_length", FloatType(), True),
    StructField("sepal_width", FloatType(), True),
    StructField("petal_length", FloatType(), True),
    StructField("petal_width", FloatType(), True),
    StructField("variety", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("species", StringType(), True),
    StructField("species_id", IntegerType(), True),
    StructField("measurement_id", IntegerType(), True),
    StructField("measurement_date", TimestampType(), True),
    StructField("measurement_notes", StringType(), True),
    StructField("measurement_type", StringType(), True),
    StructField("measurement_type_id", IntegerType(), True),
    StructField("measurement_type_name", StringType(), True),
])
# Start writing code
df= iris
grouped_df=df.groupBy(df['variety']).agg(count(df['sepal_length']).alias('count'))
grouped_df= grouped_df.orderBy('count',ascending=True)
grouped_df.toPandas()
