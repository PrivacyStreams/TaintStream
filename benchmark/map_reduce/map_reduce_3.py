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
    emails = emails.join(persons, emails.Sender == persons.Name, how="inner")

    target_schema = StructType([
        StructField("col_1", StructType([
            StructField('Sender', StringType(), True),
            StructField('Receiver', StringType(), True),
        ])),
        StructField("col_2", StructType([
            StructField('SubjectList', ArrayType(StringType()), True),
            StructField('EmailCount', StringType(), True),
        ]))
    ])
    
    emails = emails.rdd.map(lambda x: ((x[0], x[1]), ([x[2]], 1))).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).toDF()
    emails = emails.select(
        col("_1._1").alias("Sender"),
        col("_1._2").alias("Receiver"),
        col("_2._1").alias("SubjectList"),
        col("_2._2").alias("EmailCount"),
    )

    emails.printSchema()
    emails.show()
    emails = emails.coalesce(1)
    emails.write.format("json").mode("overwrite").save(output_dir)

    ed = time.time()
    print(f"[benchmark info] {__file__} finished in {ed-st}s")

