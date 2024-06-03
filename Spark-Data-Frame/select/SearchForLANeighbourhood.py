# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types  import *
airbnb_search_details= StructType(
    [


    ]
)
df = airbnb_search_details
df = df.filter(col('city') == 'LA').select(col('neighbourhood')
                                           ).distinct().orderBy(df['neighbourhood'], ascending=True)
df.dropna().toPandas()
