SELECT
    DATE_FORMAT(TRANS_DATE, '%Y-%m') as month,
    country,
    count(distinct id) as trans_count,
    count(case when state = 'approved' then 1 end) as approved_count,
    coalesce(sum(amount), 0) as trans_total_amount,
    coalesce(
        sum(case when state = 'approved' then amount end),
        0
    ) as approved_total_amount
from
    transactions
group by
    month,
    country
order by
    month