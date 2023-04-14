Use dualcore; 
-CREATE VIEW prod_orders AS
SELECT o.cust_id, o.order_id, p.price
FROM  orders o
JOIN order_details d 
    ON(o.order_id = d.order_id)
JOIN products p 
    ON (d.prod_id = p.prod_id);


SELECT count(*) 
FROM prod_orders
GROUP BY cust_id
HAVING sum(price) > 300000;
