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

counts = text_file.flatMap(lambda line: line.split("\n"))\
		.map(lambda a: (a[a.index("\t")+1:].split("\t")[0],int(a[a.index("\t")+1:].split("\t")[1])))\
		.reduceByKey(lambda a, b: a + b).sortByKey(1)



counts.saveAsTextFile(sys.argv[2])
