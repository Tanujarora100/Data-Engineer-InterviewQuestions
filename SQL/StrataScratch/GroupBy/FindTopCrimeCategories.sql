SELECT 
    category, 
    COUNT(category) AS n_occurrences
FROM 
    sf_crime_incidents_2014_01
WHERE 
    YEAR(date) = 2014
GROUP BY 
    category
ORDER BY 
    n_occurrences DESC;