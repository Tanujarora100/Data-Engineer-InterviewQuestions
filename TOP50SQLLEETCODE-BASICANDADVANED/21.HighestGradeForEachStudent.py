SQL_QUERY = """
WITH CTE AS(
SELECT STUDENT_ID, COURSE_ID, GRADE, RANK() OVER(PARTITION BY STUDENT_ID ORDER BY GRADE DESC, COURSE_ID ASC) AS RNK
FROM Enrollments
)
SELECT STUDENT_ID,COURSE_ID,GRADE FROM CTE WHERE 
RNK=1;
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

student = StructType(
    [
        StructField("student_id", IntegerType(), True),
        StructField("course_id", IntegerType(), True),
        StructField("grade", IntegerType(), True),
    ]
)
window = Window.partitionBy("student_id").orderBy(desc("grade"), "course_id")
df = student
df = student.withColumn("rank", rank().over(Window))
df = df.filter(col("rank") == 1).select("name", "student_id", "grade")
df.show()
