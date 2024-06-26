SELECT 
    SUM(CASE WHEN DAY_OF_WEEK=6 OR DAY_OF_WEEK=7 THEN 1 END) AS weekend_cnt,
    SUM(CASE WHEN DAY_OF_WEEK<6 THEN 1 END) AS working_cnt
FROM(
SELECT *,
EXTRACT(ISODOW FROM submit_date ) AS DAY_OF_WEEK
FROM TASKS) AS SUB_QUERY
