import sys
from pyspark import SparkContext, SparkConf

def set_spark_context(appName):
	conf = SparkConf().setAppName(appName).set("spark.hadoop.yarn.resourcemanager.address","127.0.0.1:8032")
	sc = SparkContext(conf=conf)
	return sc

def get_header_and_data_from_csv(sc,read_csv):
	complete_csv_file = sc.textFile(read_csv)
	csv_separated_in_lines = complete_csv_file.map(lambda line: line.split(","))
	header = csv_separated_in_lines.first()
	data = csv_separated_in_lines.filter(lambda row: row != header)
	return header,data

if __name__ == "__main__":
	# readFile: file to read words from, target_folder: where to save results, appName: name of app
	read_csv = "/home/nkostopoulos/insouline.csv"
	appName = "logistic regression, demonstration of MLlib"

	sc = set_spark_context(appName)
	header,data = get_header_and_data_from_csv(sc,read_csv)
	
