select
    id_host,
    avg(ds_checkin :: DATE - ts_booking_at :: DATE) as days
from
    airbnb_contacts
group by
    1
having
    avg(ds_checkin :: DATE - ts_booking_at :: DATE) >= 0
order by
    days desc;