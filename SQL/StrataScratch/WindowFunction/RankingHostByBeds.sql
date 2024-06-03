select
    host_id,
    sum(n_beds) as number_of_beds,
    dense_rank() over(
        order by
            sum(n_beds) desc
    ) as rank
from
    airbnb_apartments
group by
    1
order by
    rank asc