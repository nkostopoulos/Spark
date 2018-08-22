# based on https://github.com/jadianes/spark-py-notebooks/blob/master/nb10-sql-dataframes/nb10-sql-dataframes.ipynb
# dataset from https://github.com/MateLabs/Public-Datasets/blob/master/Datasets/iris.csv
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext,Row
import pyspark.mllib
import pyspark.mllib.regression
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LogisticRegressionWithSGD

def set_spark_context(appName):
	conf = SparkConf().setAppName(appName).set("spark.hadoop.yarn.resourcemanager.address","127.0.0.1:8032")
	sc = SparkContext(conf=conf)
	sc.setLogLevel("WARN")
	return sc

def map_species_to_numbers(specie):
	if specie == "Iris-setosa":
		return 1
	else:
		return 2

if __name__ == "__main__":
	# read_csv: the csv to read the training data from
	read_csv = "/home/nkostopoulos/flower2.csv"
	appName = "demonstration of SparkSQL"
	sc = set_spark_context(appName)

	# creation of an RDD using sc
	raw_data = sc.textFile(read_csv).cache()	
	# drop the first line. That line contains the header
	header = raw_data.first()
	raw_data = raw_data.filter(lambda x:x != header)

	sqlContext = SQLContext(sc)
	
	csv_data = raw_data.map(lambda l:l.split(","))
	row_data = csv_data.map(lambda p: Row(
		sepal_length = p[0],
		sepal_width = p[1],
		petal_length = p[2],
		petal_width = p[3],
		species = int(map_species_to_numbers(p[4]))
		)
	)
	flowers_df = row_data.toDF()

	cols = ["species","sepal_length","sepal_width","petal_length","petal_width"]

	flowers_df = flowers_df[cols]
	flowers_df.show(100)

	# prepare an RRD of labeled points in the format of a dictionary
	temp = flowers_df.rdd.map(lambda line:LabeledPoint(line[0],[line[1:]]))	
	print(temp.take(5))

	trainingData,testingData = temp.randomSplit([0.8,0.2],seed=1234)
	linearModel = LogisticRegressionWithSGD.train(trainingData,1000,0.2)
	print(linearModel.weights)
	print(linearModel.predict([1.5,2.0,1.2,1.4]))
