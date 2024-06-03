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
where
    rnk = 1;