select
    gender,
    count(distinct appointmentid) as n_appointments
from
    medical_appointments
group by
    1
order by
    n_appointments desc
limit
    1;