with cte as(
    select
        p.location,
        u.language,
        count(distinct u.user_id) as n_speakers
    from
        playbook_events as p
        join playbook_users as u on p.user_id = u.user_id
    group by
        1,
        2
    order by
        p.location asc
)
select
    *
from
    cte;