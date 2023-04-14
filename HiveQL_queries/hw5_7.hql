USE dualcore; 

CREATE VIEW purchasers AS
SELECT c.cust_id, o.order_id 
FROM orders o 
RIGHT OUTER JOIN customers c
ON (o.cust_id = c.cust_id);

SELECT cust_id FROM purchasers WHERE order_id IS NULL;
