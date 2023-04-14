from pyspark import SparkContext
import sys
import re
if __name__ == "__main__":
  if len(sys.argv) != 3:
     print >> sys.stderr, "Usage: hw4.py <input> <output>"
     exit(-1)
  sc = SparkContext()
  def sqrDist(point1, point2):
  	return (point1[0] -point2[0])**2 + (point1[1]-point2[1])**2
  
  def getCenter(centers, point):
	currMin = sqrDist(point, centers[0])
	centerIndex = 0
  	for i in range(1, len(centers)): 
		dist = sqrDist(point, centers[i])
		if dist < currMin:
			currMin = dist
			centerIndex = i
	return centerIndex
  def getConv(currCenters, newCenters):
	conv = 0 #placeholder value 
  	for i in range(len(currCenters)):
		conv += sqrDist(currCenters[i],newCenters[i])
	return conv


  K = 5
  convergeDistance = 0.1
  tempDist = 10
  textfile = sc.textFile(sys.argv[1])
  dataPoints = textfile.map(lambda line: line.split(",")) \
  	      .map(lambda fields: (float(fields[3]), float(fields[4]))) \
	      .filter(lambda point: point != (0,0))\
              .persist()
  centers = dataPoints.takeSample(False, K, 34)
  while(tempDist > convergeDistance): 
  	pointsByCenter = []
  	for center in centers:
  		pointsByCenter.append([center])
  
  	for point in dataPoints.collect():
  		pointsByCenter[getCenter(centers, point)].append(point)
  
  	meanList = []
  	for i in range(0,len(pointsByCenter)):
  		xList,yList = zip(*pointsByCenter[i])
		avX = sum(xList)/len(xList)
		avY = sum(yList)/len(yList)
		meanList.append((avX,avY))
  
  	tempDist = getConv(centers, meanList)
  	if tempDist > convergeDistance:  #Need to remove "if tempDist > convergeDistance:". Otherwise, the centers are not updated in the last iteration.
  		centers = meanList
 	
  rd2 = sc.parallelize(centers)	
  rd2.saveAsTextFile(sys.argv[2])
  sc.stop()
