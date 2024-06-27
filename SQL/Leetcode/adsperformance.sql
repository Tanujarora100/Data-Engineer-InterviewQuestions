WITH CTE AS (
    SELECT
        AD_ID,
        COUNT(*) AS ACTION_COUNT,
        'Viewed' AS ACTION_TYPE
    FROM
        ADS
    WHERE
        ACTION = 'Viewed'
    GROUP BY
        AD_ID
    UNION ALL
    SELECT
        AD_ID,
        COUNT(*) AS ACTION_COUNT,
        'Clicked' AS ACTION_TYPE
    FROM
        ADS
    WHERE
        ACTION = 'Clicked'
    GROUP BY
        AD_ID
)
SELECT
    *,
    COUNT(
        CASE WHEN ACTION_TYPE = 'Viewed' THEN 1 ELSE 0 END
    ) OVER(PARTITION BY AD_ID) as Viewed_count,
    COUNT(
        CASE WHEN ACTION_TYPE = 'Clicked' THEN 1 ELSE 0 END
    ) OVER(PARTITION BY AD_ID) as Clicked_count
FROM
    CTE