USE dualcore;

SELECT COUNT(customers.cust_id) 
FROM customers, orders, orders_details, products
WHERE customers.cust_id = orders.cust_id AND orders.order_id = orders_details.order_id AND orders_details.prod_id = products.prod_id
GROUP BY customers.cust_id
HAVING SUM(price) > 300000;
