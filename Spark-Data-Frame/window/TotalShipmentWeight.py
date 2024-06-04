from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Start writing code
amazon_shipment = StructType(
    [
        StructField("shipment_id", IntegerType(), True),
        StructField("weight", IntegerType(), True),
        StructField("ship_date", TimestampType(), True),
        StructField("ship_mode", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("product_base_price", IntegerType(), True),
        StructField("discount", IntegerType(), True),
        StructField("tax", IntegerType(), True),
        StructField("shipping_cost", IntegerType(), True),
        StructField("profit", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
    ]
)
window_spec = Window.partitionBy("shipment_id")
amazon_shipment = amazon_shipment.withColumn(
    "total_weight", sum("weight").over(window_spec)
)
result = amazon_shipment.toPandas()
