import random
from pyspark import *

def inside(p):
	x, y =  random.random(), random.random()
	return x*x + y*y < 1

count = sc.parallelize(xrange(0,1000000)).filter(inside).count()
print(4.0 * count / 1000000)
