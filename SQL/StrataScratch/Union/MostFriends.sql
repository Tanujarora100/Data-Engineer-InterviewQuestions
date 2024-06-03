with cte as(
    select
        user_id,
        friend_id
    from
        google_friends_network
    union
    select
        friend_id,
        user_id
    from
        google_friends_network
)
select
    user_id
from
    cte
group by
    user_id
having
    count(friend_id) > 3