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

    person_schema = StructType([
        StructField('Name', StringType(), True),
        StructField('EmailAddress', StringType(), True),
        StructField('FirstName', StringType(), True),
        StructField('LastName', StringType(), True),
        StructField('PhoneNumber', StringType(), True),
        StructField('Job', StringType(), True),
        StructField('GUID', StringType(), True),
        StructField('Company', StringType(), True),
        StructField('Age', IntegerType(), True),
    ])

    persons = spark.read.json(person_input_dir, schema=person_schema)
    persons.printSchema()

    emails = emails.where(col("Sender").isNotNull())
    sender_persons = persons.withColumn("Sender", col("Name")).select(
        col("Sender"),
        col("EmailAddress").alias("SenderEmailAddress"),
        col("job").alias("SenderJob")
    )
    emails = emails.join(sender_persons, "Sender", how="right")
    receiver_persons = persons.withColumn("Receiver", col("Name")).select(
        col("Receiver"),
        col("EmailAddress").alias("ReceiverEmailAddress"),
        col("job").alias("ReceiverJob")
    )
    emails = emails.join(receiver_persons, "Receiver", how="right")

    emails = emails.filter(col("Subject").isNull())

    emails = emails.select(
        "SenderEmailAddress",
        "SenderJob",
        "ReceiverEmailAddress",
        "ReceiverJob",
        "Subject",
    )
    
    row_num = emails.count()
    print("row number:", row_num)

    emails.show()
    emails = emails.coalesce(1)
    emails.write.format("json").mode("overwrite").save(output_dir)

    ed = time.time()
    print(f"[benchmark info] {__file__} finished in {ed-st}s")

