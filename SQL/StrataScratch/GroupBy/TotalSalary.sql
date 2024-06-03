select department, sum(salary)
from worker
group by department
order by sum(salary) desc;