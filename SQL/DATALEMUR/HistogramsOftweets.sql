with cte as(
    SELECT
        user_id,
        count(tweet_id) as tweet_count_per_user
    from
        tweets
    where
        extract(
            year
            from
                tweet_date
        ) = 2022
    group by
        1
)
select
    tweet_count_per_user as tweet_bucket,
    count(user_id) as user_nums
from
    cte
group by
    1;