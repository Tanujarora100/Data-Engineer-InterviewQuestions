from pyspark.sql.functions import *
from pyspark.sql.types import *

sf_public_salaries = StructType([
        
])
df = sf_public_salaries
df = df.filter(col("jobtitle").like("%CAPTAIN%"))
df = df.select("employeename", "basepay").toPandas()
