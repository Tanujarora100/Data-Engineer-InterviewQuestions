WITH CTE AS(
    SELECT
        P.ID,
        P.TITLE,
        ROUND(AVG(P.BUDGET) / COUNT(E.EMP_ID)) AS TOTAL_BUDGET_ALLOCATED
    FROM
        ms_projects AS P
        JOIN ms_emp_projects AS E ON P.ID = E.PROJECT_ID
    GROUP BY
        1,
        2
)
select
    title as project,
    TOTAL_BUDGET_ALLOCATED
from
    cte
order by
    TOTAL_BUDGET_ALLOCATED desc