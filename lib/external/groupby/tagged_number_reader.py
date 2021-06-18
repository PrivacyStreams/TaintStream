from pyspark.sql import SparkSession
from pyspark.sql.types import *

def tagged_number_reader():
    input_dir = "./fake_tagged_number_data"

    spark = SparkSession.builder.getOrCreate()
    schema = StructType([
        StructField('Byte', StructType([
                StructField('value', ByteType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('Short', StructType([
                StructField('value', ShortType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('Int', StructType([
                StructField('value', IntegerType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('Long', StructType([
                StructField('value', LongType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('Float', StructType([
                StructField('value', FloatType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('Double', StructType([
                StructField('value', DoubleType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('Binary', StructType([
                StructField('value', BinaryType(), True),
                StructField('tag', BooleanType(), True)
            ]), True),
        StructField('Decimal', StructType([
                StructField('value', DecimalType(38, 18), True),
                StructField('tag', BooleanType(), True)
            ]), True),
    ])
    
    emails = spark.read.json(input_dir, schema=schema)
    return emails