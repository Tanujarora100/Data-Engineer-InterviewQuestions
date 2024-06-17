select
    e.user_id
from
    emails as e
    join texts as t on t.email_id = e.email_id
where
    t.action_date - e.signup_date = Interval '1 day'
    and t.signup_action = 'Confirmed'