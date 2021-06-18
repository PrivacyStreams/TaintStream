import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time
import re
import json 

DEFAULT_WHITE_LISTED_WORDS = [
    'presentation',
    'ppt',
    'powerpoint',
    'deck',
    'slides',
    'story',
    'review',
    'status',
    'meeting',
    'agenda',
    'outline',
    'structure',
    'summary',
    'table of contents',
]


email_input_dir = "./fake_email_data"
person_input_dir = "./fake_person_data"
output_dir = f"{__file__[:-3]}_output/"

class PresentationIntentDetector:

    def __init__(self, target_words):
        self.target_words = target_words

    @staticmethod
    def get_word_map(text):
        words = re.compile('\\w+').findall(text)
        word_map = dict()
        for word in words:
            if word not in word_map:
                word_map[word] = 0
            word_map[word] += 1
        return word_map

    @staticmethod
    def get_schema():
        return StructType([StructField('detected_word_count', IntegerType(), True),
                           StructField('detected_unique_word_count', IntegerType(), True),
                           StructField('detected_words', ArrayType(StringType()), True),
                           StructField('detected_word_distribution', MapType(StringType(), IntegerType()), True)])

    def detect_intent_words(self, input_text):
        if "[taint]" in input_text:
            return input_text
        transformed = input_text.lower()
        input_word_map = PresentationIntentDetector.get_word_map(transformed)
        response_map = dict()
        response_map['detected_word_count'] = 0
        response_map['detected_unique_word_count'] = 0
        response_map['detected_words'] = set()
        response_map['detected_word_distribution'] = dict()
        for target_word in self.target_words:
            if target_word in input_word_map:
                response_map['detected_unique_word_count'] += 1
                response_map['detected_word_count'] += input_word_map[target_word]
                response_map['detected_words'].add(target_word)
                response_map['detected_word_distribution'][target_word] = input_word_map[target_word]
        response_map['detected_words'] = list(response_map['detected_words'])
        return json.dumps(response_map)

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
    emails = emails.where(~(col("IsRead")&col("IsDraft")))
    emails = emails.join(persons, emails.Sender == persons.Name, how="inner")

    whitelisted_words = DEFAULT_WHITE_LISTED_WORDS
    intent_detector = PresentationIntentDetector(whitelisted_words)
    intent_udf = udf(intent_detector.detect_intent_words, StringType())

    emails = emails.select(intent_udf(emails["UniqueBody"]).alias("intent_response_json"))
    
    emails.show()
    emails = emails.coalesce(1)
    emails.write.format("json").mode("overwrite").save(output_dir)

    ed = time.time()
    print(f"[benchmark info] {__file__} finished in {ed-st}s")

