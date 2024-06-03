WITH CTE AS(
    SELECT
        business_id,
        business_name,
        SUM(
            CASE WHEN risk_category IS NOT NULL THEN 1 ELSE 0 END
        ) AS TOTAL_VIOLATIONS
    FROM
        sf_restaurant_health_violations
    WHERE
        business_name LIKE '%school%'
        AND RISK_CATEGORY IS NOT NULL
    GROUP BY
        1,
        2
    ORDER BY
        TOTAL_VIOLATIONS DESC
)
SELECT
    business_name,
    TOTAL_VIOLATIONS AS number_of_violations
FROM
    CTE