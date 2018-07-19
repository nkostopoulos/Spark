# based on https://github.com/jadianes/spark-py-notebooks/blob/master/nb10-sql-dataframes/nb10-sql-dataframes.ipynb
# dataset from https://github.com/MateLabs/Public-Datasets/blob/master/Datasets/iris.csv
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext,Row

def set_spark_context(appName):
	conf = SparkConf().setAppName(appName).set("spark.hadoop.yarn.resourcemanager.address","127.0.0.1:8032")
	sc = SparkContext(conf=conf)
	return sc

if __name__ == "__main__":
	# readFile: file to read words from, target_folder: where to save results, appName: name of app
	read_csv = "/home/nkostopoulos/flower.csv"
	appName = "demonstration of SparkSQL"

	sc = set_spark_context(appName)
	raw_data = sc.textFile(read_csv).cache()	
	sqlContext = SQLContext(sc)
	
	csv_data = raw_data.map(lambda l:l.split(","))
	row_data = csv_data.map(lambda p: Row(
		sepal_length = p[0],
		sepal_width = p[1],
		petal_length = p[2],
		petal_width = p[3],
		species = p[4]
		)
	)
	
	flowers_df = sqlContext.createDataFrame(row_data)
	flowers_df.registerTempTable("flowers")

	#tcp_interactions = sqlContext.sql("""SELECT sepal_length FROM flowers""")
	#tcp_interactions.show()
	
	#species with petal length greater than 3.0
	petal_len_over_3 = sqlContext.sql("""SELECT species FROM flowers WHERE petal_length > 1.0""")
	query_results_number = petal_len_over_3.count()
	petal_len_over_3.show(query_results_number,truncate = False)
