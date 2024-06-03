select
    count(*) as business_open
from
    yelp_business
where
    is_open = 1