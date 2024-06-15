from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

sql_query = """WITH CTE AS(
SELECT *, DENSE_RANK() OVER(ORDER BY SALARY DESC) AS RNK
FROM WORKER

)

SELECT FIRST_NAME,SALARY
FROM CTE   
WHERE RNK=1;
"""

worker = StructType(
    
)

window_spec= Window.orderBy(desc('salary'))
df= worker
df= df.withColumn('rank',dense_rank().over(window_spec))

final_df= df.filter(col('rank')==1).select('first_name','salary')
final_df.toPandas()

