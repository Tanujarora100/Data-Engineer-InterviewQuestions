with cte as (
    select
        c.name,
        count(v.id) as total_votes
    from
        candidate as c
        join vote as v on c.id = v.candidateId
    group by
        c.name
    order by
        total_votes desc
)
select
    name
from
    cte
where
    total_votes = (
        select
            max(total_votes)
        from
            cte
    )