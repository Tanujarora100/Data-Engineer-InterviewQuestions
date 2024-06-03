WITH CTE AS(
    SELECT
        H.HOST_ID,
        H.NATIONALITY,
        COUNT(*) AS TOTAL
    FROM
        airbnb_hosts AS H
        INNER JOIN airbnb_apartments AS A ON H.HOST_ID = A.HOST_ID
        AND H.NATIONALITY <> A.COUNTRY
    GROUP BY
        1,
        2
    ORDER BY
        H.HOST_ID
)
SELECT
    COUNT(*) AS COUNT
FROM
    CTE