USE dualcore;

SELECT cust_id FROM customers 
WHERE customers.cust_id != orders.cust_id
