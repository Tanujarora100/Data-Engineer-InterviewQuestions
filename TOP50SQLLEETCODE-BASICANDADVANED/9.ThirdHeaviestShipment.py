from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

amazon_shipment = StructType([
    StructField('shipment_id', IntegerType(), True),
    StructField('weight', IntegerType(), True)
])

df = amazon_shipment.groupBy('shipment_id').agg(
    sum(col('weight')).alias('total_sum'))
window_spec = Window.orderBy(desc(df['total_sum']))
grouped_df = df.withColumn('rank', dense_rank().over(window_spec))
final_df = grouped_df.filter(col('rank') == 3).drop('rank')
final_df.toPandas()
