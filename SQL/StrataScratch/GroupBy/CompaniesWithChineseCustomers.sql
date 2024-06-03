with cte as(
    select
        company_id,
        language,
        count(*) as total_speakers
    from
        playbook_users
    where
        language = 'chinese'
    group by
        1,
        2
    having
        count(*) >= 2
)
select
    company_id
from
    cte;