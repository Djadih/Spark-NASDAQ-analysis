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
			f.write(str(val1) + "," + str(val2) + "\n")


if __name__ == "__main__":
	spark = SparkSession.builder.getOrCreate()

	result = [] # a list of (companyName, #OfProfitableDays) tuples

	path = os.getcwd() + "/NASDAQ100/"
	for fileName in os.listdir(path):
		# assuming all files in "path" are .csv files
		companyName = remove_csv_suffix(fileName)

	    # Read each .csv file
		df = read_csv(path + fileName)

	    # Select only "Open" and "Close" column
		df = df.select("Open", "Close")

	    # Filter rows to retains only those rows that satisfy condition "(close - open) / open >= 0.01"
		df = custom_filter(df)

		# Output each file as one line "name,days"
		result.append((str(companyName), str(df.count())))

	# Sort output according to company name
	result.sort()

	# Write out result to "output_1.txt"
	write_output(result, "output_1.txt")
