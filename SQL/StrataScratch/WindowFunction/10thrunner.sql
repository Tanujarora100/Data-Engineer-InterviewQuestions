with cte_rank as (
    select
        person_name,
        net_time,
        dense_rank() over(
            order by
                net_time asc
        ) as rn
    from
        marathon_male
)
select
    net_time - (
        select
            net_time
        from
            cte_rank
        where
            rn = 10
        limit
            1
    ) as difference
from
    marathon_male
where
    person_name = 'Chris Doe'