from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, dense_rank
from pyspark.sql.window import Window

# Filter the dataset for January
january_sales = sales_data.filter(col("month").startswith("2024-01"))

# Define the window specification
window_spec = Window.partitionBy("product_category").orderBy(col("total_sales").desc())

# Use row_number over the window spec to rank sellers within each product category
top_sellers_by_category = (
    january_sales.withColumn("rank", dense_rank().over(window_spec))
    .filter(col("rank") <= 3)
    .drop("rank")
)

# Display the result
top_sellers_by_category.toPandas()
