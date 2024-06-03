-- CTE to calculate population density for each city
WITH base_cte AS (
    SELECT
        city,
        country,
        population / area AS density
    FROM
        cities_population
    WHERE
        population > 0
        AND area > 0
),
-- CTE to rank cities by population density in descending order
cte_max AS (
    SELECT
        city,
        country,
        density,
        DENSE_RANK() OVER (
            ORDER BY
                density DESC
        ) AS max_density
    FROM
        base_cte
),
-- CTE to rank cities by population density in ascending order
cte_min AS (
    SELECT
        city,
        country,
        density,
        DENSE_RANK() OVER (
            ORDER BY
                density ASC
        ) AS min_density
    FROM
        base_cte
),
-- CTE to select the city with the highest and lowest population density
final_cte AS (
    SELECT
        city,
        country,
        density
    FROM
        cte_max
    WHERE
        max_density = 1
    UNION ALL
    SELECT
        city,
        country,
        density
    FROM
        cte_min
    WHERE
        min_density = 1
) -- Final selection ordered by density ascending
SELECT
    *
FROM
    final_cte
ORDER BY
    density ASC;