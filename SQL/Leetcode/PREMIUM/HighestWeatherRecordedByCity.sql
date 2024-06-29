SELECT
    CITY_ID,
    DAY,
    DEGREE
FROM
    (
        SELECT
            CITY_ID,
            DAY,
            DEGREE,
            DENSE_RANK() OVER(
                PARTITION BY CITY_ID
                ORDER BY
                    DEGREE DESC,
                    DAY ASC
            ) AS RNK
        FROM
            WEATHER
    ) AS SUB_QUERY
WHERE
    RNK = 1