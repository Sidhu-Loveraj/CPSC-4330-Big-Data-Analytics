#Loveraj sidhu
#CSPC 4330
#01/24/2021
#hw3.py

from pyspark import SparkContext
import re
import sys

if __name__ == "__main__":
	if len(sys.argy) !=3:
		print >> sys.stderr, "usage: hw3.py <input> <output>"
		exit(-1)

	#create Spark Context Object
	sc = SparkContext()

	#Store Input as Data
	amazonData = sc.textFile(sys.argv[1])

	#Filter header out of dataset
	firstRow = amazonData.first()
	amazonData = amazonData.filter(lambda row: row != firstRow)

	#Create RDD and perform operations: 
	#1. Split data by tab
	#2. Tuple(Prod_ID, (Rating, 1))
	#3. Aggregrate 
	#4. Map

	myrdd = amazonData.map(lambda line: line.split("\t")) \
		.map(lambda line: (line[3].encode("utf-8"), (float(line[7]), 1.0))) \
		.reduceByKey(lambda prod1, prod2: (fload(prod1[0] + prod2[1]), prod1[1] + prod2[1])) \
		.map(lambda (prod, rate): (prod, (float(rate[0]) / rate[1], rate[1]))) \
		.sortByKey()

	#Stop SC and save
	myrdd.saveAsTextFile(sys.argv[2])
	sc.stop()