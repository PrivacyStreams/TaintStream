import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time
import re


email_input_dir = "./fake_email_data"
person_input_dir = "./fake_person_data"
output_dir = f"{__file__[:-3]}_output/"


# Tokenizer specific constants
TOKENIZER_TRAINING_RULE = {"'s": " 's", "'ve": " 've", "'t": " 't", "'re": " 're", "'d": " 'd", "'ll": " 'll",
                            '’': " '", ',': ' , ', '"': ' " ', '!': ' ! ', '(': ' ( ', ')': ' ) ', '?': ' ? ',
                            '.': ' . ', ':': ' : ', ';': ' ; '}

TOKENIZER_TRAINING_SEPERATOR = " "


class Token:
    """Unit object for the output of tokenization (word token)"""
    def __init__(self, text=""):
        self.text = text

    def ispunct(self):
        """
        Checks whether the token is consisted with punctuations

        Arguments:
            N/A
        Returns:
            True if all characters are punctuations, otherwise False {bool}
        """
        for char in self.text:
            if char not in string.punctuation:
                return False
        return True

    def isspace(self):
        """
        Checks whether all characters in the token are whitespace characters

        Arguments:
            N/A
        Returns:
            True if all characters are whitespace characters, otherwise False {bool}
        """
        return self.text.isspace()

    def __str__(self):
        return self.text


def char_tokenizer(input_str: str, sep_chars="", del_chars=""):
    """
    Python version of CharTokenizer in mlgtools
    First parse based on characters in sepChars and then remove characters in delChars for each token
    Arguments:
        input_str {str} -- input string to tokenize
        sep_chars {str} -- characters to be used for tokenization (string)
        del_chars {str} -- tokenization characters to be removed after tokenization (string)
    Returns:
        Tuple (list of tokens, list of intertokens)
    """
    regex_sepchars_compiled = None
    regex_delchars_compiled = None

    if sep_chars:
        regex_sepchars = "[" + sep_chars + "]+"
        regex_sepchars_compiled = check_and_compile_regular_expression(regex_sepchars)

    if del_chars:
        regex_delchars = "[" + del_chars + "]+"
        regex_delchars_compiled = check_and_compile_regular_expression(regex_delchars)

    # Tokenize with sep_chars, generate output intertokens & tokens (Token object)
    output_tokens = []
    output_intertokens = []

    if regex_sepchars_compiled is not None:
        prev_idx = 0
        match_iter = regex_sepchars_compiled.finditer(input_str)

        try:
            cur_match = next(match_iter)

            # If the first match does not start from the beginning, add an empty intertoken
            if cur_match.start() != 0:
                output_intertokens.append(Token())

            while True:
                try:
                    # If there is a match from the beginning, do not produce a token
                    if cur_match.start() != 0:
                        output_tokens.append(Token(input_str[prev_idx:cur_match.start()]))

                    # Add matched parts into intertokens
                    output_intertokens.append(Token(cur_match.group()))
                    prev_idx = cur_match.end()
                    cur_match = next(match_iter)

                except StopIteration:
                    break

        except StopIteration:
            # This means that there is no match. We just add an empty intertoken
            output_intertokens.append(Token())

        # Write leftovers to tokens, and add empty intertoken
        if prev_idx != len(input_str):
            output_tokens.append(Token(input_str[prev_idx:]))
            output_intertokens.append(Token())

    else:
        # If there is no separator chars, write whole string a single token
        output_tokens.append(Token(input_str))

    # delete chars from delChars in tokens/intertokens
    if regex_delchars_compiled:
        for i, output_token in enumerate(output_tokens):
            output_tokens[i] = Token(regex_delchars_compiled.sub('', output_token.text))
        for i, output_intertoken in enumerate(output_intertokens):
            output_intertokens[i] = Token(regex_delchars_compiled.sub('', output_intertoken.text))

    return output_tokens, output_intertokens


# --------------------------------------------------------------------------------------------
# Tokenizer base class
class Tokenizer:
    """
    Base Class for Tokenization.
    Child class should implement tokenize_into_words,  function.
    """
    # --------------------------------------------------------------------------------------------
    # APIs for tokenization

    def tokenize_into_words(self, input_string: str):
        """
        Virtual function that implements the word tokenization

        Arguments:
            input_string -- input string to tokenize
        Returns:
            list of words {Token}
        """
        raise NotImplementedError()


class TrainingTokenizer(Tokenizer):
    """
    Tokenizer used for training LM. Mimics one implemented in QCS.
    It first replaces substring based on rules in replacements dictionary,
    and then tokenizes based on separator_chars
    This is the default one we are using for all tokenization on SmartCompose.

    Arguments:
            replacements {dict} -- dictionary rules to replace substring from the input string
            separator_chars {str} -- each char in the string will be used to tokenize input string
    """
    def __init__(self, replacements=TOKENIZER_TRAINING_RULE,
                 separator_chars=TOKENIZER_TRAINING_SEPERATOR):
        Tokenizer.__init__(self)

        # Place longer ones first to keep shorter substrings from matching where the longer ones should take place
        # For instance given the replacements {'ab': 'AB', 'abc': 'ABC'} against the string 'hey abc', it should produce
        # 'hey ABC' and not 'hey ABc'
        self.replacements = replacements
        replacements_sorted_list = sorted(replacements, key=len, reverse=True)
        # Create a big OR regex that matches any of the substrings to replace
        self.replacements_regex_str = '|'.join(map(re.escape, replacements_sorted_list))
        self.separator_chars = separator_chars

    def tokenize_into_words(self, input_string: str):
        """
        Word tokenizer for 'training' tokenizer. First replaces substring with the rules in replacements dictionary,
        and then tokenizes based on separtor_chars

        Arguments:
            input_string {str} -- string to tokenize
        Returns:
            Tuple {(list of tokens, list of intertokens)}
        """
        replaced_strs = multi_replace(input_string, self.replacements, self.replacements_regex_str)
        return char_tokenizer(replaced_strs, sep_chars=self.separator_chars, del_chars='')


def multi_replace(input_str: str, replacements: dict, replacements_regex_str: str):
    """
    Given a string and a replacement map, it returns the replaced string.
    Code was from https://gist.github.com/bgusach/a967e0587d6e01e889fd1d776c5f3729

    Arguments:
        input_str {str} -- string to execute replacements on
        replacements {dict} -- replacement dictionary {value to find: value to replace}
        replacements_regex_str {str} -- regex that matches any of the substrings to replace
    Returns:
        filtered string {str}
    """
    # re automatically uses a cache of compiled re by running re.compile
    regex_compiled = check_and_compile_regular_expression(replacements_regex_str)
    # For each match, look up the new string in the replacements
    return regex_compiled.sub(lambda match: replacements[match.group()], str(input_str))


def check_and_compile_regular_expression(regex_str: str):
    """
    Compiles the regular expression string
    If it fails to compile, raise a ReCompileError
    Arguments:
        regex_str {str} -- regular expression to compile
    Returns:
        compiled regular expression object {Pattern}
    """
    try:
        # According to https://docs.python.org/3/library/re.html, Python will cache most recent compiled regular expression, 
        # so we don't do an implicit caching for re.compile output
        regex_compiled = re.compile(regex_str)
    except:
        raise

    return regex_compiled


def tokenizer_for_training(input_str, replacements=TOKENIZER_TRAINING_RULE, separator_chars=TOKENIZER_TRAINING_SEPERATOR):
    result = []
    try:
        training_tokenizer = TrainingTokenizer(replacements=replacements, separator_chars=separator_chars)
        result, _ = training_tokenizer.tokenize_into_words(input_str)
        result = [str(token) for token in result]
    except:
        except_details = traceback.format_exc()
        except_details = except_details.replace("\n", "\t")
        print("SystemLog: Exception in intersect "+except_details)
        raise
    return result

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

    tokenizer_udf = udf(tokenizer_for_training, ArrayType(StringType()))

    email_with_tokens = emails.select("UniqueBody", tokenizer_udf(lower(col("UniqueBody"))).alias("words1"))
    
    email_with_tokens.show()
    email_with_tokens = email_with_tokens.coalesce(1)
    email_with_tokens.write.format("json").mode("overwrite").save(output_dir)

    ed = time.time()
    print(f"[benchmark info] {__file__} finished in {ed-st}s")

