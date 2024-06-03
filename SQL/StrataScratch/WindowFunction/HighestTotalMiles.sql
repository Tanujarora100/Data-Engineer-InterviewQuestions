with cte as(
    select
        sum(miles) as total_miles,
        purpose,
        rank() over(
            order by
                sum(miles) desc
        ) as rnk
    from
        my_uber_drives
    where
        category = 'Business'
    group by
        2
)
select
    purpose,
    total_miles
from
    cte
where
    rnk < 4