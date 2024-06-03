


WITH cte AS (
SELECT airbnb_hosts.host_id as host_id,airbnb_hosts.nationality as nationality, airbnb_units.unit_type as unit_type
from 
    airbnb_hosts
join 
    airbnb_units
on
    airbnb_hosts.host_id=airbnb_units.host_id
),
ranked_table as(
select nationality, count(distinct host_id) 
    as all_units, 
        dense_rank() 
            over
            (order by unit_type desc) rank_order
        from 
            cte
        )

select nationality, count(all_units) as apartments
from ranked_table
where all_units='Apartment' and rank_order=1
group by nationality;