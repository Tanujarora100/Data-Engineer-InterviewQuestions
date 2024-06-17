/* https://www.youtube.com/watch?v=eMQDHHfUJtU&list=PLBTZqjSKn0IfuIqbMIqzS-waofsPHMS0E&index=19 */
WITH CTE AS(
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY CID
            ORDER BY
                CID ASC
        ) AS FLIGHT_TABLE
    FROM
        FLIGHTS
),
CTE2 AS(
    SELECT
        CID,
        ORIGIN
    FROM
        CTE
    WHERE
        FLIGHT_TABLE = 1
),
DESTINATION AS(
    SELECT
        CID,
        DESTINATION
    FROM
        CTE
    WHERE
        FLIGHT_TABLE = (
            SELECT
                MAX(FLIGHT_TABLE)
            FROM
                CTE
        )
)
SELECT
    CTE2.CID,
    ORIGIN,
    DESTINATION
FROM
    CTE2
    JOIN DESTINATION ON CTE2.CID = DESTINATION.CID;