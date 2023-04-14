USE dualcore;

SELECT avg(rating) as avg_rating, prod_id
FROM ratings 
GROUP BY prod_id
HAVING count(rating) > 49 
ORDER BY avg_rating ASC LIMIT 1;
