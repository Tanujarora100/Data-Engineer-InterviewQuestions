# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start writing code
fct_customer_sales= StructType([
    StructField('customer_id', IntegerType(), True),
    StructField('prod_sku_id', IntegerType(), True),
    StructField('order_date', DateType(), True),
    StructField('order_quantity', IntegerType(), True),
    StructField('order_amount', FloatType(), True),
])

dim_product= StructType([

])

joined_df=fct_customer_sales.join(dim_product,on='prod_sku_id',how='right')
filtered_df = joined_df.filter(col('order_date').isNull()).select('prod_sku_id', 'market_name')

filtered_df.toPandas()