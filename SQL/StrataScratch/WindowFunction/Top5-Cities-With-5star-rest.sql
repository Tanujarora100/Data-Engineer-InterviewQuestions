with
    cte as (
        select
            city,
            count(*) as count_of_5_stars
        from
            yelp_business
        where
            stars = 5
        group by
            city
    ),
    ranked_cte as (
        select
            *,
            rank() over (
                order by
                    count_of_5_stars desc
            ) as rnk
        from
            cte
    )
select
    city,
    count_of_5_stars
from
    ranked_cte
where
    rnk <= 5