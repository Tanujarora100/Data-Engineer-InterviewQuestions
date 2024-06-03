WITH CTE AS(
    SELECT
        u.company_id,
        c.user_id,
        dense_rank() over(
            partition by u.company_id
            order by
                count(distinct c.call_id) desc
        ) as rnk
    from
        rc_calls as c
        join rc_users as u on c.user_id = u.user_id
    group by
        1,
        2
)
select
    *
from
    cte
where
    rnk <= 2
order by
    rnk asc,
    user_id desc