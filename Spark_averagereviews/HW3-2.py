from pyspark import SparkContext
import sys
import re
if __name__ == "__main__":
  if len(sys.argv) != 3:
     print >> sys.stderr, "Usage: average.py <input> <output>"
     exit(-1)
  sc = SparkContext()
  textfile = sc.textFile(sys.argv[1])
  avgReview = textfile.map(lambda line: line.split("\t")) \
             .filter(lambda data: "marketplace" not in data[0]) \
             .map(lambda att: (att[3].encode("utf-8"), int(att[7]))) \
             .groupByKey() \
             .sortByKey() \
             .map(lambda col: \
		(col[0], float(sum(col[1]/len(col[1]))), len(col[1])))

  avgReview.saveAsTextFile(sys.argv[2])
  sc.stop()
