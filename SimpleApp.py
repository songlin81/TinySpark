"""SimpleApp.py"""
from pyspark import SparkContext

logFile = "/Users/wwh/Downloads/spark-2.1.0-bin-hadoop2.7/README.md"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i, lines with b: %i, line count: %i" % (numAs, numBs, logData.count()))
