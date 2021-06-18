import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time


input_dir = "./fake_number_data"
output_dir = f"{__file__[:-3]}_output/"

if __name__ == "__main__":
    st = time.time()    
    spark = SparkSession.builder.master("local[1]") \
                .appName('benchmark') \
                .getOrCreate()
    
    schema = StructType([
        StructField('Byte', ByteType(), False),
        StructField('Short', ShortType(), False),
        StructField('Int', IntegerType(), False),
        StructField('Long', LongType(), False),
        StructField('Float', FloatType(), False),
        StructField('Double', DoubleType(), False),
        StructField('Binary', BinaryType(), False),
        StructField('Decimal', DecimalType(38, 18), False),
    ])
    
    numbers = spark.read.json(input_dir, schema=schema)
    numbers.printSchema()
    
    numbers = numbers.agg(
        stddev_pop(col("Float")).alias("stddev_pop(Float)"),
        sum("Int").alias("IntSum"),
        avg("Int").alias("IntAvg"),
        sum("Short").alias("ShortSum"),
        avg("Short").alias("ShortAvg"),
    )

    numbers.printSchema()
    # numbers.show()
    numbers = numbers.coalesce(1)
    numbers.write.format("json").mode("overwrite").save(output_dir)

    ed = time.time()
    print(f"[benchmark info] {__file__[:-3]} finished in {ed-st}s")

