WITH base AS (
    SELECT
        *
    FROM
        olympics_athletes_events
    WHERE
        team = 'China'
        and (
            year between 2000
            and 2016
        )
        and season = 'Summer'
)
SELECT
    medal,
    case when year = 2000 then count(medal) else 0 end as medals_2000,
    case when year = 2004 then count(medal) else 0 end as medals_2004,
    case when year = 2008 then count(medal) else 0 end as medals_2008,
    case when year = 2012 then count(medal) else 0 end as medals_2012,
    case when year = 2016 then count(medal) else 0 end as medals_2016,
    count(medal) as total_medals
from
    base
group by
    1,
    year
order by
    total_medals desc;