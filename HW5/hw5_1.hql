USE dualcore;

CREATE TABLE IF NOT EXISTS loyalty_programTable
	(cust_id INT,
	f_name STRING,
	l_name STRING,
	email STRING,
	phone_num MAP<STRING, STRING>,
	order_id ARRAY<INT>,
	order_stats STRUCT<min:INT, 
				max:INT, 
				avg:INT, 
				totalVal:INT>
)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

DESCRIBE loyalty_programTable;
