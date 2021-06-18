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

    users = emails.select(
                      lower(col("Sender")).alias("Sender"),
                      lower(col("SenderAddress")).alias("SenderAddress"),
                      lower(col("Receiver")).alias("Receiver"),
                      lower(col("ReceiverAddress")).alias("ReceiverAddress"),
                      lower(col("Puser")).alias("Puser"),
                      length(col("Puser").alias("PuserLength"))
                    )
    distinct_sender = users.select("Sender").distinct().count()
    distinct_receiver = users.select("Receiver").distinct().count()
    distinct_puser = users.select("Puser").distinct().count()

    print(f"distinct sender num: {distinct_sender}")
    print(f"distinct receiver num: {distinct_receiver}")
    print(f"distinct guid num: {distinct_puser}")

    relation = users.distinct()
    relation = relation.orderBy("Sender", "SenderAddress", "Receiver", "ReceiverAddress", "Puser")
    
    # relation.show()
    relation = relation.coalesce(1)
    relation.write.format("json").mode("overwrite").save(output_dir)

    ed = time.time()
    print(f"[benchmark info] {__file__} finished in {ed-st}s")

