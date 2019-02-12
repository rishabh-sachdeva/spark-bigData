from pyspark import SparkContext
import sys
import shutil
import os

#author - Rishabh Sachdeva

sc = SparkContext("local", "app")

text_file = sc.textFile(sys.argv[1])

#delete output directory if exists
if os.path.exists(sys.argv[2]):
	shutil.rmtree(sys.argv[2])

pair_1 = text_file.flatMap(lambda line: line.split("\n"))\
		.map(lambda a: (a.split("\t")[0],a.split("\t")[1]))\
		.groupByKey()\
		.mapValues(list)

pair_2 = text_file.flatMap(lambda line: line.split("\n"))\
		.map(lambda a: (a.split("\t")[1],a.split("\t")[0]))\
		.groupByKey()\
		.mapValues(list)

#merge two RDDs using join
pair_joined=pair_2.join(pair_1)

#fetch common attributes from  lists across key using filter function
final_pair = pair_joined.map(lambda x: tuple(x))\
	.map(lambda y: (y[0], list(set(y[1][0])\
	.intersection(y[1][1])))).filter(lambda z: len(z[1])>0 )\
	.sortByKey()


final_pair.saveAsTextFile(sys.argv[2])
