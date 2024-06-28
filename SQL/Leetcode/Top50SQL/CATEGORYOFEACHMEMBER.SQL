with CTE as (
    select
        member_id,
        (count(charged_amount) / count(visit_date) * 100) as conversion_rate
    from
        Visits v
        left join Purchases p on v.visit_id = p.visit_id
    group by
        member_id
)
SELECT
    C.MEMBER_ID,
    M.NAME,
    CASE WHEN conversion_rate >= 80 THEN "Diamond" WHEN conversion_rate >= 50
    and conversion_rate < 80 THEN "Gold" WHEN conversion_rate < 50 THEN "Silver" WHEN CONVERSION_RATE = 0 THEN 'Bronze' END AS category
FROM
    CTE AS C
    LEFT JOIN Members AS M ON C.MEMBER_ID = M.MEMBER_ID