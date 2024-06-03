with cte as(
select user_id, 
    sum(case when session_type='streamer' then 1 else 0 end) 'streaming_count',
    sum(case when session_type='viewer' then 1 else 0 end) 'viewer_count',
        count(*) as 'total'
    from twitch_sessions
    group by user_id
)

select user_id, streaming_count as streaming, viewer_count as 'VIEW'
    from cte
        where streaming_count > viewer_count
        order by cte.total desc
    limit 10;