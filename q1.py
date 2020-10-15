import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains

import os

def read_csv(file_name):
    df = spark.read.option("header",True).csv(file_name)
    return df

def write_csv(df):
    df.write.option("header",True) \
        .csv("/home/hussam/Documents/spark/tmp.csv")

def custom_filter(df):
    filtered_df = df.filter((df.Close - df.Open) / (df.Open) >= 0.01)
    filtered_df.show(truncate=False)
    return filtered_df

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# os.chdir("/home/hussam/Documents/spark/NASDAQ100/")

d = "/home/hussam/Documents/spark/NASDAQ100/"
for fileName in os.listdir(d):
    fileName = os.path.join(d, fileName)

    print(fileName)
    df = read_csv(fileName)
    df.printSchema()
    specific_df = df.select("Open", "Close")
    specific_df.printSchema()
    filtered_df = custom_filter(df)
    print("There are", filtered_df.count(),"rows.")