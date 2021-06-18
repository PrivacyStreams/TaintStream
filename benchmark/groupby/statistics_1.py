import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time


input_dir = "./fake_email_data"
output_dir = f"{__file__[:-3]}_output/"

if __name__ == "__main__":
    st = time.time()    
    spark = SparkSession.builder.master("local[1]") \
                .appName('benchmark') \
                .getOrCreate()
    
    schema = StructType([
        StructField('Sender', StringType(), True),
        StructField('SenderAddress', StringType(), True),
        StructField('Receiver', StringType(), True),
        StructField('ReceiverAddress', StringType(), True),
        StructField('Subject', StringType(), True),
        StructField('UniqueBody', StringType(), True),
        StructField('SentDateTime', DateType(), True),
        StructField('Puser', StringType(), True),
        StructField('IsRead', BooleanType(), True),
        StructField('IsDraft', BooleanType(), True),
        ])
    
    emails = spark.read.json(input_dir, schema=schema)
    emails.printSchema()

    ymd = emails.select(length(col("UniqueBody")).alias("body_length"), year(col("SentDateTime")).alias("year"), month(col("SentDateTime")).alias("month"), dayofmonth(col("SentDateTime")).alias("day"))
    ymd = ymd.groupBy(col("year")).agg(
        sum(col("body_length")).alias('sum of body_length'), 
        # min(col("body_length")).alias('min of body_length'), 
        max(col("body_length")).alias('max of body_length'),
        mean(col("body_length")).alias('avg of body_length'),
        # sqrt(mean(col("body_length")*2)-mean("body_length")).alias('sd')
    )
    ymd.printSchema()
    
    ymd.show()
    ymd = ymd.coalesce(1)
    ymd.write.format("json").mode("overwrite").save(output_dir)

    ed = time.time()
    print(f"[benchmark info] {__file__[:-3]} finished in {ed-st}s")

