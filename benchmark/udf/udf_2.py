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

    @udf(BooleanType())
    def contains_words(row):
        word_list = ["Interesting", "away", "Become", "entire", "mind", "sign"]
        for word in word_list:
            if word in row:
                return True
        return False
    
    @udf(StringType())
    def word_count(row):
        return row

    emails = emails.where(~(col("IsRead")&col("IsDraft")))
    emails = emails.filter(contains_words(col("UniqueBody"))) \
                   .withColumn("word_count", word_count(col("UniqueBody"))) \
                   .drop("UniqueBody", "Email_ID")
                   
    emails = emails.select(
        "word_count"
    )

    row_num = emails.count()
    print("row number:", row_num)
    
    emails.show()
    emails = emails.coalesce(1)
    emails.write.format("json").mode("overwrite").save(output_dir)

    ed = time.time()
    print(f"[benchmark info] {__file__} finished in {ed-st}s")

