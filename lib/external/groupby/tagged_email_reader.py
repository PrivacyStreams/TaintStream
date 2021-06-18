from pyspark.sql import SparkSession
from pyspark.sql.types import *

def tagged_email_reader():
    input_dir = "./fake_tagged_email_data"

    spark = SparkSession.builder.getOrCreate()
    schema = StructType([
        StructField('Sender', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('SenderAddress', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('Receiver', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('ReceiverAddress', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('Subject', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('UniqueBody', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('SentDateTime', StructType([
                StructField('value', DateType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('Puser', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('IsRead', StructType([
                StructField('value', BooleanType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('IsDraft', StructType([
                StructField('value', BooleanType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
    ])
    
    emails = spark.read.json(input_dir, schema=schema)
    return emails