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

    emails = emails.where(~(col("IsRead")&col("IsDraft"))).select("Sender", "UniqueBody")
    emails = emails.where((emails['UniqueBody'].isNotNull()) & (length(emails['UniqueBody']) > 0))
    female_emails = emails.where(emails["Sender"].contains("Mrs."))
    female_cnt = female_emails.count()
    male_emails = emails.where(emails["Sender"].contains("Mr."))
    male_cnt = male_emails.count()
    unknown_emails = emails.where(~(emails["Sender"].contains("Mr.") | emails["Sender"].contains("Mrs.")))
    unknown_cnt = unknown_emails.count()

    print(f"female email cnt: {female_cnt}, male email cnt: {male_cnt}, unknown: {unknown_cnt}")


    # female_emails.show()
    unknown_emails = unknown_emails.coalesce(1)
    unknown_emails.write.format("json").mode("overwrite").save(output_dir)

    ed = time.time()
    print(f"[benchmark info] {__file__} finished in {ed-st}s")

