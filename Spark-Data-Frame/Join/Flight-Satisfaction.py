# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
# Start writing code
survey_results= StructType([
    StructField('cust_id', IntegerType(), True),
    StructField('satisfaction', StringType(), True),
    StructField('type_of_travel', StringType(), True),
    StructField('class', IntegerType(), True),
    StructField('flight_distance', IntegerType(), True),
    StructField('departure_delay_in_minutes', IntegerType(), True),
    StructField('arrival_delay_in_minutes', IntegerType(), True)
])

loyalty_customers= StructType([
    StructField('cust_id', IntegerType(), True),
    StructField('age', IntegerType(), True),
    StructField('gender', StringType(), True),
])
df= survey_results
df= df.join(loyalty_customers, loyalty_customers.cust_id==survey_results.cust_id,'inner')
# df.show()
df=df.filter(col('age').between(30,40))
grouped_df= df.groupBy(col('class')).agg(round(avg(col('satisfaction'))).alias('avg_score'))
grouped_df.toPandas()
# To validate your solution, convert your final pySpark df to a pandas df
# survey_results.toPandas()