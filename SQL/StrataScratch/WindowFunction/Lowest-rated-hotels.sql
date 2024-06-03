WITH
    ranked AS (
        SELECT
            hotel_name,
            average_score,
            RANK() OVER (
                ORDER BY
                    average_score
            ) AS rnk
        FROM
            (
                SELECT DISTINCT
                    hotel_name,
                    average_score
                FROM
                    hotel_reviews
            ) AS distinct_hotels
    )
SELECT
    hotel_name,
    average_score
FROM
    ranked
WHERE
    rnk < 11;