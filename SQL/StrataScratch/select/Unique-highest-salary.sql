select
    max(salary)
from
    employee
where
    salary in (
        select
            salary
        from
            employee
        group by
            salary
        having
            count(salary) = 1
    )