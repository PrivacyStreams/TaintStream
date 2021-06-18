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
        StructField('Receiver', ArrayType(StringType()), True),
        StructField('Ccs', ArrayType(StringType()), True),
        StructField('Content', StructType([
            StructField('Subject', StringType(), True),
            StructField('UniqueBody', StringType(), True),
        ])),
        StructField('SentDateTime', DateType(), True),
        StructField('Email_ID', StringType(), True),
        StructField('IsRead', BooleanType(), True),
        StructField('IsDraft', BooleanType(), True),
        StructField('Word2Index', MapType(StringType(), IntegerType()), True)
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
        StructField('Profile', StringType(), True), # a json string
    ])

    persons = spark.read.json(person_input_dir, schema=person_schema)
    persons.printSchema()

    emails = emails.join(persons, emails.Sender == persons.Name, how="inner")

    emails = emails.withColumn(
        "RealProfile",
        from_json(col("Profile"), StructType([
            StructField('job', StringType(), True),
            StructField('company', StringType(), True),
            StructField('ssn', StringType(), True),
            StructField('residence', StringType(), True),
            StructField('current_location', ArrayType(StringType()), True),
            StructField('blood_group', StringType(), True),
            StructField('website', ArrayType(StringType()), True),
            StructField('username', StringType(), True),
            StructField('name', StringType(), True),
            StructField('sex', StringType(), True),
            StructField('address', StringType(), True),
            StructField('mail', StringType(), True),
            StructField('birthdate', StringType(), True),
        ]))
    ).drop("Profile")

    emails = emails.groupBy("Sender").agg(
        mean(length("Content.UniqueBody")).alias("content_length"),
        first(length(col("RealProfile.address"))).alias("address"),
        first(col("RealProfile")).alias("Profile")
    )
    
    
    emails.show()
    emails.printSchema()
    emails = emails.limit(1000).coalesce(1)
    emails.write.format("json").mode("overwrite").save(output_dir)

    ed = time.time()
    print(f"[benchmark info] {__file__} finished in {ed-st}s")

