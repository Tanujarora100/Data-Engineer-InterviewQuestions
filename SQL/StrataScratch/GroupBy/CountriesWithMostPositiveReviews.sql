SELECT
    reviewer_nationality,
    SUM(
        CASE WHEN LOWER(positive_review) NOT LIKE 'No Positive' THEN 1 ELSE 0 END
    ) as n_positive_reviews
FROM
    hotel_reviews
GROUP BY
    reviewer_nationality
HAVING
    n_positive_reviews > 0
ORDER BY
    n_positive_reviews DESC