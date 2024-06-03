with cte as(
    select
        o.cust_id,
        concat(c.first_name, ' ', c.last_name) as full_name,
        o.order_details
    from
        customers as c
        left join orders as o on c.id = o.cust_id
        /* All Rows from customers will be shown here */
)
select
    count(*) as n_customers_without_orders
from
    cte
where
    cust_id is NULL;
/* Count those rows where the cust_id is null because that is the foreign key for joining
 */