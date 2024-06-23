with cte as (
    select
        *
    from
        actions
    where
        action_date = date_sub('2019-07-05', Interval 1 day)
        and action = 'report'
)
select
    extra as report_reason,
    count(distinct post_id) as report_count
from
    cte
group by
    report_reason
order by
    report_count desc