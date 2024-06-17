select * from countries;
select * from families;
-- maximum discounts a family can get 
with cte as(
select count(*) as total_discounts from families as f
join countries as c
on f.family_size>=c.min_size
group by f.name
order by total_discounts desc
limit 1)
select total_discounts as max_discounts_of_family from cte;

-- name of the family with the maximum discounts
select f.name, count(*) as total_discounts
	from families as f
join countries as c
on f.family_size>= c.min_size
group by f.name 
order by total_discounts desc;

-- Families which fulfill both the conditions
select f.name, count(*) as max_discounts
from families as f
join countries as c
on f.family_size>=c.min_size and f.family_size<=c.max_size
group by f.name
order by max_discounts desc;
