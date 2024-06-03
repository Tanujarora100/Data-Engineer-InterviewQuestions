# Import your libraries
import pyspark.sql.functions as F

df=los_angeles_restaurant_health_inspections
df= df.groupBy('grade').agg(F.mean('score').alias('avg_score'))
df.show()
df.toPandas()