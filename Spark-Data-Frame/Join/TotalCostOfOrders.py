from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import findspark
findspark.init('/opt/homebrew/Cellar/apache-spark/3.5.1/libexec')

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Restaurant Database").getOrCreate()
spark = SparkSession.builder.appName("Restaurant Database").getOrCreate()
customers = StructType([
    StructField('id', IntegerType(), True),
    StructField('first_name', StringType(), True),
    StructField('last_name', StringType(), True),
    StructField('email', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('address', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('zip', IntegerType(), True)
])
orders = StructType([
    StructField('id', IntegerType(), True),
    StructField('cust_id', IntegerType(), True),
    StructField('order_date', StringType(), True),
    StructField('total_order_cost', IntegerType(), True),
    StructField('order_status', StringType(), True),
    StructField('payment_mode', StringType(), True),
    StructField('payment_status', StringType(), True),
    StructField('shipping_date', StringType(), True),
    StructField('item_count', IntegerType(), True),
    StructField('items', StringType(), True),
    StructField('shipping_cost', IntegerType(), True),
    StructField('shipping_state', StringType(), True),
    StructField('shipping_city', StringType(), True),
    StructField('shipping_zip', IntegerType(), True),
    StructField('shipping_country', StringType(), True),
])

df = customers

merged_df = customers.join(orders, customers.id == orders.cust_id)
merged_df = merged_df.groupBy('cust_id', 'first_name').agg(
    sum('total_order_cost').alias('total_order_cost'))
merged_df.orderBy('first_name', ascending=True).toPandas()
