USE dualcore;

LOAD DATA LOCAL INPATH 
'/home/cloudera/training_materials/analyst/exercises/data_mgmt/loyalty_data.txt'
INTO TABLE loyalty_programTable;

SELECT * from loyalty_programTable;
	
