SELECT  EMP.NAME AS EMPLOYEE
    FROM EMPLOYEE EMP, EMPLOYEE MGR
    WHERE EMP.MANAGERID=MGR.ID
    AND 
        EMP.SALARY > MGR.SALARY;
