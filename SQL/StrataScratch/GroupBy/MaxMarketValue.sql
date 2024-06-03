WITH CTE AS(
SELECT SECTOR, MAX(MARKETVALUE) AS MAX_MARKETVALUE
FROM forbes_global_2010_2014
GROUP BY SECTOR
ORDER BY MAX_MARKETVALUE DESC
)
SELECT SECTOR AS 'sector', MAX_MARKETVALUE as 'max_marketvalue'
    FROM CTE;