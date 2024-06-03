
with cte as(
    select f.flight_id, f.flight_duration,
    m.duration, m.movie_id
    from flight_schedule as f
    join
    entertainment_catalog as m
    on m.duration<=f.flight_duration
)
select flight_id, movie_id, duration as movie_duration
    from cte
    where flight_id=101
    and duration < flight_duration
    order by movie_duration;