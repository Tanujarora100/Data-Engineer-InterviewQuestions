WITH cte AS (
    SELECT
        user_id,
        visit_date,
        LEAD(visit_date, 1, '2021-01-01') OVER (
            PARTITION BY user_id
            ORDER BY
                visit_date
        ) AS NEXT_DAY
    FROM
        uservisits
)
select
    user_id,
    max(datediff(NEXT_DAY, visit_date)) as biggest_window
from
    cte
group by
    user_id