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

    emails = emails.where(col("Sender").isNotNull()).filter(~(col("IsRead")&col("IsDraft")))
    emails = emails.join(persons, emails.Sender == persons.Name, how="inner")

    @udf(IntegerType())
    def word_count(row):
        return len(row.split())

    emails = emails.select(
        "Content",
        # "Content.Subject",
        # "Content.UniqueBody",
        word_count("Content.Subject").alias("Subject_word_count"),
        word_count("Content.UniqueBody").alias("UniqueBody_word_count"),
    )
    
    emails.show()
    emails = emails.coalesce(1)
    emails.write.format("json").mode("overwrite").save(output_dir)

    ed = time.time()
    print(f"[benchmark info] {__file__} finished in {ed-st}s")

