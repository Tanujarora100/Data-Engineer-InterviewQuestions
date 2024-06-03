select
    stars,
    count(distinct review_id) as n_entries
from
    yelp_reviews
group by
    1
order by
    stars asc;