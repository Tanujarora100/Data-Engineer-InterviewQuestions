# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark= SparkSession.builder\
.appName('Step Count').getOrCreate()
# Start writing code
facebook_product_features_realizations
df= facebook_product_features_realizations
df= df.groupBy(col('feature_id')).agg(max(col('step_reached')).alias('max_step_reached'))
# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()