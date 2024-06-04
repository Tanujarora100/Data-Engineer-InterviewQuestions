WITH cte AS(
    SELECT
        c.ACCOUNT_ID,
        a.gender,
        a.age,
        COUNT(c.ACCOUNT_ID) as total_claims
    FROM
        cvs_claims AS c
        INNER JOIN cvs_accounts as a ON a.account_id = c.account_id
    WHERE
        YEAR(c.date_submitted) = 2021
    GROUP BY
        1
    HAVING
        total_claims > 1
)
select
    gender,
    round(avg(age), 0) as avg_age
from
    cte
group by
    gender;