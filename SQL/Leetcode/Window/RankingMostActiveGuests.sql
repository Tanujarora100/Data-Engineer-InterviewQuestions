SELECT
    RNK,
    ID_GUEST,
    TOTAL_MESSAGES
FROM
    (
        SELECT
            ID_GUEST,
            SUM(n_messages) AS TOTAL_MESSAGES,
            DENSE_RANK() OVER(
                ORDER BY
                    SUM(n_messages) DESC
            ) AS RNK
        FROM
            airbnb_contacts
        GROUP BY
            ID_GUEST
    ) AS SUB_QUERY