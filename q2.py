import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains
import os

def read_csv(file_name):
    return spark.read.option("header",True).csv(file_name)

def custom_filter(df):
    return df.filter((df.Close - df.Open) / (df.Open) >= 0.01)

def remove_csv_suffix(fileName):
	return fileName[:-4]

def write_output(result, outputFileName):
	# assume result contains (val1, val2) tuples
	with open(outputFileName, "w") as f:
		for val1, val2 in result:
			f.write(str(val1) + "," + "[" + ",".join(val2) + "]\n")

if __name__ == "__main__":
	spark = SparkSession.builder.getOrCreate()

	path = os.getcwd() + "/NASDAQ100/"

	# A list of (companyName, dataFrame) tuples
	allCVSs = [(remove_csv_suffix(fileName), read_csv(path + fileName)) for fileName in os.listdir(path)]
	allCVSs = [(companyName, df.select("Open", "Close", "Date")) for companyName, df in allCVSs]

	# Assume all .csv files have the same number of rows
	numRows = allCVSs[0][1].count()


	result = []
	for i in range(numRows):
		date = str(allCVSs[0][1].collect()[i].Date)

		profits = [] # a list of (companyName, profit percentages for that company) tuples

		for companyName, df in allCVSs:
			row = df.collect()[i]
			profit = (float(row["Close"]) - float(row["Open"])) / float(row["Open"])
			profits.append((companyName, profit))

		profits.sort(key=lambda x : -x[1]) # sort according to profit in descending order
		result.append((date, [x[0] for x in profits[:5]]))
	write_output(result, "output_2.txt")