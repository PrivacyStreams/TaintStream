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

    emails = emails.withColumn("Read&Draft", col("IsRead")&col("IsDraft"))
    emails = emails.filter(~col("Read&Draft"))
    emails = emails.drop(col("Read&Draft"))

    greetingPatternRegex = r'^[^\w]*(hi there|hi|hello|hey|dear|greetings|good afternoon|good morning|good evening)(^|\s|\W)'

    emails = emails.withColumn("FirstSentence", lower(split(col('UniqueBody'), r'[.,;!?\n]')[0])) \
                   .withColumn("StartingWord", lower(trim(regexp_extract(col('UniqueBody'), r'\w*\b', 0)))) \
                   .withColumn("Greeting", lower(trim(regexp_extract(col('FirstSentence'), greetingPatternRegex, 0))))
    
    emails = emails.select("FirstSentence")
    # emails.show()
    emails = emails.coalesce(1)
    emails.write.format("json").mode("overwrite").save(output_dir)

    ed = time.time()
    print(f"[benchmark info] {__file__} finished in {ed-st}s")

