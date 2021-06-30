#Loveraj Sidhu
#CPSC 4330
#03/05/2021
#hw4.py



from pyspark import SparkContext
import sys
import math

#Squared Distances between two points		
def distanceSquared(p1,p2):
	return (p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2

#Sum of two points
def addPoints(p1, p2):
	return [p1[0] + p2[0], p1[1] + p2[1]]

#Closest point - return index in the array of points that is closest to p
def closestPoint(p, pointsArr):
	index = 0
	closest = float
	for i in range(len(pointsArr)):
		distance = distanceSquared(p, pointsArr[i])
		if distance < closest:
			closest = distance
			index = i
	return index

#Compute average cluster 
def average(p,clus):
	return (p[0]/clus,p[1]/clus)

#Main Functionality 
if __name__ == "__main__":
	if len(sys.argv) != 3:
		print >> sys.stderr, "Usage: hw4.py <input> <output>"
		exit(-1)
	sc = SparkContext()

	#Load data file
	data = sc.textFile(sys.argv[1])

	#Set center points to 5
	numCP = 5
	 
	#Amount of locations of the means changes between iterations
	convergeDist = 0.1
	
	#Parse data into pairs. Get Latitude and Longitude. Filter out 0,0 location
	pairRDD = data.map(lambda line: line.split(',')) \
		.map(lambda pair: (float(pair[3]), (float(pair[4])))) \
		.filter(lambda p: p != (0,0)) \
		.persist()
	
	#Sample K points
	kPoints = pairRDD.takeSample(False, numCP, 34)

	#Iterate until total distance between 1 point is less than the convergeDist
	tempDist = float("inf")
	while(tempDist > convergeDist):
		#Get closest point in data set
		cpRDD = pairRDD.map(lambda p: (closestPoint(p, kPoints), (p,1)))
		#Sum Points up 
		addRDD = cpRDD.reduceByKey(lambda p1, p2: (addPoints(p1[0], p2[0]), p1[1] + p2[1]))
		#Finew new center point
		avgRDD = addRDD.map(lambda p: (p[0], average(p[1][0], p[1][1])))

		newCP = avgRDD.collect()
		
		tempDist = 0.0
		for i in range(len(kPoints)):
			tempDist += distanceSquared(newCP[i][1], kPoints[i])
			kPoints[i] = newCP[i][1]

	#Store Results in output file
	result = sc.parallelize(kPoints)
	#Stop SC and save
	result.saveAsTextFile(sys.argv[2])
	sc.stop()
