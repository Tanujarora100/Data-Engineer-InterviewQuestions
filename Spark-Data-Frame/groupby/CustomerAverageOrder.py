# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

postmates_orders = StructType(
    [
        StructField("order_amount", IntegerType(), False),
        StructField("date_order", DateType(), False),
        StructField("customer_id", IntegerType(), False),
    ]
)
df = postmates_orders
ans = postmates_orders.agg(
    count_distinct("customer_id").alias("unique_customers"),
    mean("amount").alias("mean_amount"),
)
ans.toPandas()
