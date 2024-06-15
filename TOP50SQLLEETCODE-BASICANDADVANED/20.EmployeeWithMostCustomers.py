from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

map_employee_territory = StructType(
    [
        StructField("empl_id", IntegerType(), True),
        StructField("territory_id", IntegerType(), True),
    ]
)
map_customer_territory = StructType(
    [
        StructField("cust_id", IntegerType(), True),
        StructField("territory_id", IntegerType(), True),
        StructField("total_customers", IntegerType(), True),
        StructField("total_employees", IntegerType(), True),
    ]
)
employee_territory = map_employee_territory.alias("emp")

customer_territory = map_customer_territory.alias("cust")

joined_df = employee_territory.join(
    customer_territory, col("emp.territory_id") == col("cust.territory_id"), how="inner"
)
window = Window.orderBy(desc("total_customers"))

ans_df = joined_df.groupBy(col("empl_id")).agg(
    countDistinct("cust_id").alias("total_customers")
)
ans_df = ans_df.withColumn("rank", dense_rank().over(window))
ans_df.filter(col("rank") == 1).drop("rank").toPandas()


sql_query="""
WITH cte AS(
    SELECT
        E.empl_id,
        E.territory_id,
        COUNT(map_customer_territory.cust_id) AS TOTAL_CUSTOMERS,
        RANK() OVER(
            ORDER BY
                COUNT(map_customer_territory.cust_id) DESC
        ) AS RNK
    FROM
        map_employee_territory AS E
        INNER JOIN map_customer_territory ON map_customer_territory.territory_id = E.territory_id
    GROUP BY
        1,
        2
)
SELECT
    empl_id,
    total_customers as n_customers
from
    cte
where rnk=1;

"""