with cte as(
    select
        nominee,
        count(*) as n_movies_by_abi
    from
        oscar_nominees
    where
        nominee = 'Abigail Breslin'
    group by
        1
)
select
    n_movies_by_abi
from
    cte;