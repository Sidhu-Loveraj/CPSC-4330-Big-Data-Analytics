USE dualcore;

SELECT COUNT(prod_id) AS total_products
FROM customers, orders, orders_details
WHERE customers.cust_id = 1071189
AND customers.cust_id = orders.cust_id AND orders.order_id = orders_details.order_id;
