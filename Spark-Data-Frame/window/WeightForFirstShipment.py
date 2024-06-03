from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
# Start writing code
amazon_shipment = StructType([
    StructField('shipment_id', IntegerType(), False),
    StructField('weight', IntegerType(), True),
    StructField('shipment_date',DateType(), False)
])

window_spec = Window.partitionBy('shipment_id').orderBy('shipment_date')
df = amazon_shipment
df = df.withColumn('rank', row_number().over(window_spec))
final_df = df.filter(col('rank') == 1).select(df['shipment_id'], df['weight'])
final_df.toPandas()
