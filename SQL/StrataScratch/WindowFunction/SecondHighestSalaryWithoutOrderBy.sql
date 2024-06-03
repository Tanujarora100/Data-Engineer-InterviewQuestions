WITH CTE AS(
    SELECT
        distinct salary,
        DENSE_RANK() OVER(
            ORDER BY
                SALARY DESC
        ) AS RNK
    FROM
        WORKER
)
SELECT
    salary
from
    cte
where
    rnk = 2;