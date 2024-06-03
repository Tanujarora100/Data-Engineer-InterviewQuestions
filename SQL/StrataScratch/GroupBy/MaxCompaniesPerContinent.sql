with cte as(
    select
        continent,
        count(distinct company) as n_companies
    from
        forbes_global_2010_2014
    group by
        1
)
select
    continent,
    n_companies
from
    cte
where
    n_companies =(
        select
            max(n_companies)
        from
            cte
    )