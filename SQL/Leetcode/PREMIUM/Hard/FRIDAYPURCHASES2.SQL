WITH CTE AS (
    SELECT
        *
    from(
            SELECT
                *,
                DAYNAME(PURCHASE_DATE) AS DAY_OF_WEEK,
                ROUND((DAY(PURCHASE_DATE) -1) / 7) + 1 AS WEEK_OF_MONTH
            FROM
                PURCHASES
        ) as a
    WHERE
        DAY_OF_WEEK = 'Friday'
)
SELECT
    WEEK_OF_MONTH,
    PURCHASE_DATE,
    COALESCE(SUM(AMOUNT_SPEND), 0) AS TOTAL_AMOUNT
FROM
    CTE
GROUP BY
    WEEK_OF_MONTH,
    PURCHASE_DATE
ORDER BY
    WEEK_OF_MONTH