from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, row_number, month
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Filter the dataset for January
sales_data = StructType([])
january_sales = sales_data.filter(month(col("month")) == 1)


window_spec = Window.partitionBy("product_category").orderBy(desc("total_sales"))
filtered_df = january_sales.withColumn("rnk", row_number().over(window_spec))
filtered_df = filtered_df.filter(col("rnk") <= 3).drop(col("rnk")).toPandas()
