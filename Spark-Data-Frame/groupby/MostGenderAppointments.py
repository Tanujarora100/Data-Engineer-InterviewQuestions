# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start writing code

medical_appointments = StructType([
    StructField('patientid', IntegerType(), True),
    StructField('appointmentid', IntegerType(), True),
    StructField('gender', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('neighbourhood', StringType(), True),
    StructField('scholarship', IntegerType(), True),
    StructField('hipertension', IntegerType(), True),
    StructField('diabetes', IntegerType(), True),
    StructField('alcoholism', IntegerType(), True),
    StructField('handcap', IntegerType(), True),
    StructField('sms_received', IntegerType(), True),
    StructField('no_show', StringType(), True),
    StructField('scheduled_day', TimestampType(), True),
    StructField('appointment_day', TimestampType(), True),
    StructField('wait_time', IntegerType(), True),
])
df = medical_appointments
df = df.groupBy('gender').agg(
    count(col('patientid')).alias('total_appointments'))
final_df = df.orderBy('total_appointments', ascending=False).limit(1)
final_df.toPandas()


# To validate your solution, convert your final pySpark df to a pandas df
# medical_appointments.toPandas()
