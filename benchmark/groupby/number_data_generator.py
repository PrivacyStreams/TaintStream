from faker import Faker
import random
import json
import os
import sys
import datetime
import decimal
fake = Faker()


first_row = True


user_num = 100
row_num = 10000
save_every = 500

# Byte: int
# Short: int
# Int: int
# Long: int
# Binary: binary
# Float: float
# Double: float

output_dir = "./fake_number_data"
tagged_output_dir = "./fake_tagged_number_data"



class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj,datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        else:
            return json.JSONEncoder.default(self,obj)

def new_row():
    global first_row
    row = {} # original input data for scripts
    tagged_row = {} # tagged input data for scripts

    fields = [
        "Byte",
        "Short",
        "Int",
        "Long",
        "Binary",
        "Float",
        "Double",
        "Decimal"
    ]
    first_row = random.random() < 0.5
    if first_row:
        Byte = fake.pyint(min_value=-100, max_value=100)
        ByteTag = True
        Short = fake.pyint(min_value=-10000, max_value=10000)
        ShortTag = False
        Int = fake.pyint(min_value=-100000, max_value=100000)
        IntTag = True
        Long = fake.pyint(min_value=-1000000, max_value=1000000)
        LongTag = True
        Binary = fake.pystr()
        BinaryTag = False
        Float = fake.pyfloat()
        FloatTag = True
        Double = fake.pyfloat()
        DoubleTag = False
        Decimal = fake.pydecimal()
        DecimalTag = False
        first_row = False
    else:
        Byte = 0
        ByteTag = False
        Short = 0
        ShortTag = False
        Int = 0
        IntTag = False
        Long = 0
        LongTag = False
        Binary = fake.pystr()
        BinaryTag = False
        Float = 0
        FloatTag = False
        Double = 0
        DoubleTag = False
        Decimal = 0
        DecimalTag = False

    locs = locals().copy()
    for field in fields:
        row[field] = locs[field]
        tagged_row[field] = {"value":locs[field], "tag": locs[field+"Tag"]}

    return row, tagged_row

if __name__ == "__main__":
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    if not os.path.exists(tagged_output_dir):
        os.mkdir(tagged_output_dir)
    
    file_no = 0
    file_no_str = format(file_no, "05d")
    f = open(f"{output_dir}/{file_no_str}.json", "w")
    tf = open(f"{tagged_output_dir}/{file_no_str}.json", "w")
    for tot in range(row_num):
        row, tagged_row = new_row()
        row_str = json.dumps(row, cls=DateEncoder)
        tagged_row_str = json.dumps(tagged_row, cls=DateEncoder)
        f.write(row_str + "\n")
        tf.write(tagged_row_str + "\n")
        if (tot+1) % save_every == 0:
            f.close()
            file_no += 1
            file_no_str = format(file_no, "05d")
            f = open(f"{output_dir}/{file_no_str}.json", "w")
            tf = open(f"{tagged_output_dir}/{file_no_str}.json", "w")
            print(f"generate {tot+1} rows...")
        