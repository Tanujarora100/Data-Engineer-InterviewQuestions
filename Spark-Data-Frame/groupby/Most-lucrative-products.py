import pyspark.sql.functions as F
from pyspark.sql.window import Window


df = online_orders.withColumn('month', F.month('date'))

# Filter for months 1 to 6 (January to June)
filtered_df = df.filter((F.col('month') >= 1) & (F.col('month') <= 6))

# Calculate total revenue per order
revenue_df = filtered_df.withColumn('total', F.col('units_sold') * F.col('cost_in_dollars'))
grouped_df=revenue_df.groupBy(F.col('product_id')).agg(F.sum('total').alias('total'))
# grouped_df.show÷())
final_df= grouped_df.orderBy(F.col('total').desc()).limit(5)
final_df.show()
final_df.toPandas()
