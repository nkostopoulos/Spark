# based on https://github.com/jadianes/spark-py-notebooks/blob/master/nb10-sql-dataframes/nb10-sql-dataframes.ipynb
# dataset from https://github.com/MateLabs/Public-Datasets/blob/master/Datasets/iris.csv
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext,Row

def set_spark_context(appName):
	conf = SparkConf().setAppName(appName).set("spark.hadoop.yarn.resourcemanager.address","127.0.0.1:8032")
	sc = SparkContext(conf=conf)
	sc.setLogLevel("WARN")
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

	print("Show everything in the database")
	temp = sqlContext.sql("SELECT * FROM flowers")
	query_results_number = temp.count()
	temp.show(query_results_number,truncate = False)
	
	print("Sepal length and sepal width of the specie 'Iris-setosa'")
	temp = sqlContext.sql("""SELECT sepal_length,sepal_width FROM flowers WHERE species == 'Iris-setosa'""")
	query_results_number = temp.count()
	temp.show(query_results_number,truncate = False)

	print("Petal length and width of the specie 'Iris-virginica' sorted in ascending order based on petal length")
	temp = sqlContext.sql("""SELECT petal_length,petal_width FROM flowers WHERE species == 'Iris-virginica' ORDER BY petal_length""")
	query_results_number = temp.count()
	temp.show(query_results_number,truncate = False)

	print("Count the records in the table that correspond to the specie 'Iris-virginica'")
	temp = sqlContext.sql("""SELECT COUNT(*) FROM flowers WHERE species == 'Iris-virginica'""")
	temp.show() 

	print("Find maximum petal length of each specie")
	temp = sqlContext.sql("""SELECT species,MAX(petal_length) AS MaximumPetalLength FROM flowers GROUP BY species""")
	query_results_number = temp.count()
	temp.show(query_results_number,truncate = False)
