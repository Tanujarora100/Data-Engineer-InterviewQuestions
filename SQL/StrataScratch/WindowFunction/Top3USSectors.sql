with cte as (
    select
        sector,
        avg(rank) as avg_rank,
        rank() over (
            order by
                avg(rank) asc
        ) as rnk
    from
        forbes_global_2010_2014
    where
        country = 'United States'
    group by
        1
)
select
    sector,
    avg_rank
from
    cte
where
    rnk < 4
order by
    avg_rank;