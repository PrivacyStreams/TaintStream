import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time


email_input_dir = "./fake_email_data"
person_input_dir = "./fake_person_data"
output_dir = f"{__file__[:-3]}_output/"

if __name__ == "__main__":
    st = time.time()    
    spark = SparkSession.builder.master("local[1]") \
                .appName('benchmark') \
                .getOrCreate()
    
    email_schema = StructType([
        StructField('Sender', StringType(), True),
        StructField('Receiver', StringType(), True),
        StructField('Subject', StringType(), True),
        StructField('UniqueBody', StringType(), True),
        StructField('SentDateTime', DateType(), True),
        StructField('Email_ID', StringType(), True),
        StructField('IsRead', BooleanType(), True),
        StructField('IsDraft', BooleanType(), True),
    ])
    
    emails = spark.read.json(email_input_dir, schema=email_schema)
    emails.printSchema()

    def complex_but_useless_func(col2, col3):
        import random
        l = [random.random() for _ in range(int(1e4))]
        l.sort()
    
    @udf
    def accept_redundant_args(col1, col2, col3):
        complex_but_useless_func(col2, col3)
        return col1

    emails = emails.select(
        accept_redundant_args(col("Sender"), col("UniqueBody"), col("Email_ID")).alias("sender_only"),
        "Email_ID",
        "UniqueBody"
    )
                   
    
    emails.show()
    emails = emails.coalesce(1)
    emails.write.format("json").mode("overwrite").save(output_dir)

    ed = time.time()
    print(f"[benchmark info] {__file__} finished in {ed-st}s")

