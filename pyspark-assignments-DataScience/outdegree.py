from pyspark import SparkContext
import sys
import os
import shutil

#author - Rishabh Sachdeva

sc = SparkContext("local", "app")
text_file = sc.textFile(sys.argv[1])

#delete output directory if exists
if os.path.exists(sys.argv[2]):
	shutil.rmtree(sys.argv[2])

outDeg = text_file.flatMap(lambda line: line.split("\n"))\
		.map(lambda a: (a[0:a.index("\t")], 1))\
		.reduceByKey(lambda a, b: a + b).sortByKey(1)

outDeg.saveAsTextFile(sys.argv[2])
