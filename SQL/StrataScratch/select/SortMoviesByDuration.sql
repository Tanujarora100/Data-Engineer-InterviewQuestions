select
    *
from
    movie_catalogue
order by
    cast(replace(duration, ' min', '') as INT) desc;