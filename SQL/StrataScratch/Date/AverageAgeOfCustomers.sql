-- Average age of gender of people who filled more than one claim in year =2021
-- gender and average age should be there rounded to nearest whole number
with cte as(
    select
        a.gender,
        round(avg(a.age)) as average_age,
        count(distinct claim_id) as total_claims
    from
        cvs_accounts as a
        join cvs_claims as c on c.account_id = a.account_id
        and YEAR(date_submitted) = 2021
    group by
        1
    having
        total_claims > 1
)
select
    *
from
    cte