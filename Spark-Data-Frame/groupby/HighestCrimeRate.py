from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark=  SparkSession.sparkContext
# Start writing code
spark= spark.builder().appName("spark").getOrCreate()
sf_crime_incidents_2014_01=StructType([
    StructField('date',StringType,True),
    StructField('day_of_week',StringType,True),
    StructField('time',StringType,True),
    StructField('category',StringType,True),
    StructField('descript',StringType,True),
    StructField('resolution',StringType,True),
    StructField('address',StringType,True),
    StructField('pddistrict',StringType,True),
    StructField('x',StringType,True),
    StructField('y',StringType,True),

])
df=sf_crime_incidents_2014_01
grouped_df= df.groupBy('day_of_week').agg(count(col('incidnt_num')).alias('total'))
ordered_df= grouped_df.orderBy('total',ascending=False).toPandas()
spark.sql("""
          select day_of_week, count(distinct incidnt_num) as total_incidents
from sf_crime_incidents_2014_01
group by 1
order by total_incidents desc""")