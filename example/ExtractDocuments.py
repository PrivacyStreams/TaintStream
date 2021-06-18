import sys
import os
import traceback
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, explode, lit, when, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, BooleanType


def logPrint(msg):
    print("SystemLog: " + str(msg))

# @compliant_handle()
def join():
    groundTruthDataFile = "data/documents_input"
    # groundTruthDataFile = path
    file_path = "data/documents_input"
    outputFile = f"{__file__[:-3]}._output.csv"

    appName = "symbolic_test"
    import time
    time_start=time.time()
    spark = SparkSession.builder.appName(appName) \
        .config('spark.jars') \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    time_end=time.time()
    print('start spark cost',time_end-time_start)

    logPrint("Load ground truth data from " + groundTruthDataFile)
    groundTruth = spark.read.json(groundTruthDataFile)

    logPrint("Load documents data from " + file_path)
    documents = spark.read.json(file_path)
    
    documents = documents.select('puser', 'FileName', 'DocumentTitle', 'Author').withColumn("UserId", regexp_replace(regexp_replace(col("Author"), "\"", ""), "\'", "")).drop("Author")

    groundTruth = groundTruth.select('Author', 'FileSize')

    documents = documents.join(groundTruth, groundTruth.Author == documents.UserId).select(documents.UserId, 'Filename', 'DocumentTitle', 'Author')
    # documents.write.csv(path=outputFile, header=True, sep='\t', mode='overwrite')
    documents.show()

    def my_add(a,b):
        return a+b

    documents = documents.rdd.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(lambda a,b: my_add(a,b)).toDF()

    documents.show(truncate=False)


if __name__ == "__main__":
    import time
    time_start=time.time()
    join()
    time_end=time.time()
    print('running cost',time_end-time_start)