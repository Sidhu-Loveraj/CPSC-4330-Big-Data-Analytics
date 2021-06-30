USE dualcore;

SELECT message FROM ratings 
WHERE prod_id = 1274673
AND message LIKE '%ten times more%' 
LIMIT 3; 
	
