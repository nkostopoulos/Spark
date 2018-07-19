import sys

from pyspark import SparkContext, SparkConf

def set_spark_context(appName):
	conf = SparkConf().setAppName(appName).set("spark.hadoop.yarn.resourcemanager.address","127.0.0.1:8032")
	sc = SparkContext(conf=conf)
	return sc

def map_reduce_to_count_word_frequency(sc,sourceFile):
  	# read in text file and split each document into words
  	words_only_letters = sc.textFile(sourceFile)
	words = words_only_letters.map(remove_non_letters)
	split_words = words.flatMap(lambda line: line.split(" "))
  	# count the occurrence of each word
  	wordCounts = split_words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a + b)
	return wordCounts

def remove_non_letters(line):
	new_line = ""
	for letter in line:
		if letter == " ":
			new_line = new_line + " "
		elif letter.isalpha() == False:
			new_line = new_line + " "
		else:
			new_line = new_line + letter
	return new_line

def print_in_ascending_order(freqs):
	sorted_words = freqs.sortBy(lambda pair: pair[1],ascending=False)
	for atuple in sorted_words.collect():
		word = atuple[0]
		freq = str(atuple[1])
		print("word " + word + " appears " + freq)
	return sorted_words

def save_results(save_item,target_folder):
	save_item.saveAsTextFile(target_folder)
	return None

def count_number_of_distinct_words(all_words):
	number_of_words = all_words.count()
	print("number of distinct words is " + str(number_of_words))
	return number_of_words

def print_top_n(sorted_words,n):
	top_n = sorted_words.take(n)
	print("Printing top " + str(n) + " words")
	for atuple in top_n:
		print("word " +	atuple[0] + " with frequency " + str(atuple[1]))
	return None

if __name__ == "__main__":
	# readFile: file to read words from, target_folder: where to save results, appName: name of app
	readFile = "/home/nkostopoulos/les_miserables.txt"
	target_folder = "/home/nkostopoulos/athlioi"
	appName = "Word frequency of Les Miserables"

	sc = set_spark_context(appName)
	word_frequencies = map_reduce_to_count_word_frequency(sc,readFile)
	sorted_words = print_in_ascending_order(word_frequencies)
	count_number_of_distinct_words(sorted_words)
	print_top_n(sorted_words,10)
	save_results(sorted_words,target_folder)


