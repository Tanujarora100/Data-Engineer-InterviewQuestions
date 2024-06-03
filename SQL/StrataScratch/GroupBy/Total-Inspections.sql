WITH CTE AS (
    SELECT
        FACILITY_CITY,
        COUNT(DISTINCT RECORD_ID) AS N_TOTAL_INSPECTIONS
    FROM
        los_angeles_restaurant_health_inspections
    WHERE
        EXTRACT (
            YEAR
            FROM
                ACTIVITY_DATE :: DATE
        ) = 2017
        AND PE_DESCRIPTION ILIKE '%LOW%'
    GROUP BY
        1
)
SELECT
    n_total_inspections
from
    cte;