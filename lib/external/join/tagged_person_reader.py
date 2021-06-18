from pyspark.sql import SparkSession
from pyspark.sql.types import *

def tagged_person_reader():
    input_dir = "./fake_tagged_person_data"

    spark = SparkSession.builder.getOrCreate()
    schema = StructType([
        StructField('Name', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('EmailAddress', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('FirstName', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('LastName', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('PhoneNumber', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('Job', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('GUID', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('Company', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('Age', StructType([
                StructField('value', IntegerType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
    ])
    
    emails = spark.read.json(input_dir, schema=schema)
    return emails