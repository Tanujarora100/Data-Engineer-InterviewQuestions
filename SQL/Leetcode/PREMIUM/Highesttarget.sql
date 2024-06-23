select
    first_name,
    target,
    bonus
from
    employee
where
    target = (
        select
            max(target)
        from
            employee
    )