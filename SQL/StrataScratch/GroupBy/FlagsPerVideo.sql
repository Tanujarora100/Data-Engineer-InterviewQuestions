with cte as(
    select
        distinct concat(user_firstname, ' ', user_lastname) as unique_name,
        video_id,
        count(flag_id) as total_flags
    from
        user_flags
    where
        flag_id is not null
    group by
        1,
        2
)
select
    video_id,
    total_flags
from
    cte
order by
    total_flags desc;