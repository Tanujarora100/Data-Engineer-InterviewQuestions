with cte as(
    select
        r.user_id,
        u.name,
        sum(r.distance) as totaldistance
    from
        lyft_rides_log as r
        join lyft_users as u on u.id = r.user_id
    group by
        1,
        2
    order by
        totaldistance desc
    limit
        10
)
select
    *
from
    cte