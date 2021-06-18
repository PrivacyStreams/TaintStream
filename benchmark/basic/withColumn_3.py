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

    emails = emails.filter(~(col("IsRead")&col("IsDraft")))
    col_name = "concat_sender"
    emails = emails.withColumn(col_name, concat_ws(";", "Sender", "SenderAddress"))
    col_name = "concat_receiver"
    emails = emails.withColumn(col_name, concat_ws("-", "Receiver", "ReceiverAddress"))
    col_name = "concat_subject_and_body"
    emails = emails.withColumn(col_name, concat_ws(":", lower(col("Subject")), lower(col("UniqueBody"))))

    emails = emails.select(
        "concat_sender",
        "concat_receiver",
        "concat_subject_and_body"
    )
    # emails.show()
    emails = emails.coalesce(1)
    emails.write.format("json").mode("overwrite").save(output_dir)

    ed = time.time()
    print(f"[benchmark info] {__file__} finished in {ed-st}s")

