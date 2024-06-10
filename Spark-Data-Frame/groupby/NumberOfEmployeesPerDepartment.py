from pyspark.sql.functions import col, count
from pyspark.sql.types import *
# Assuming you have a Spark DataFrame named 'employee'
employee_counts = spark.sql("""
SELECT department, count(*) AS total_employees
FROM employee
GROUP BY department
ORDER BY total_employees DESC
""")

employee= StructType([
    
])
df= employee
df= df.groupBy('department').agg(count('employee_id').alias('total_employees'))
final_df= df.orderBy(col('total_employees').desc())
final_df.toPandas()

# Display the results (optional)
employee_counts.show()