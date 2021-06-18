from faker import Faker
import random
import json
import os
import datetime
fake = Faker()


user_num = 100
row_num = 10000
save_every = 500
user_list = [(fake.name(), fake.email(), fake.ssn()) for _ in range(user_num)]

# Sender: name string
# SenderAddress: email string
# Receiver: name string
# ReceiverAddress: email string
# Subject: string
# UniqueBody: string
# SentDateTime: datetime
# puser: GUID string
# IsRead: boolean
# IsDraft: boolean

output_dir = "./fake_email_data"
tagged_output_dir = "./fake_tagged_email_data"


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj,datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return json.JSONEncoder.default(self,obj)

def new_row():
    row = {} # original input data for scripts
    tagged_row = {} # tagged input data for scripts
    fields = [
        "Sender",
        "SenderAddress",
        "Receiver",
        "ReceiverAddress",
        "Subject",
        "UniqueBody",
        "SentDateTime",
        "Puser",
        "IsRead",
        "IsDraft"
    ]

    sender = user_list[random.randint(0, user_num-1)]
    receiver = user_list[random.randint(0, user_num-1)]
    
    flag = random.random() > 0.5
    if flag:
        Sender = sender[0]
        SenderTag = False
        SenderAddress = "[taint]"+sender[1]
        SenderAddressTag = True # email address is considered sensitive
        Receiver = receiver[0]
        ReceiverTag = False
        ReceiverAddress = "[taint]"+receiver[1]
        ReceiverAddressTag = True # email address is considered sensitive
        IsRead = fake.pybool()
        IsReadTag = False
        IsDraft = fake.pybool()
        IsDraftTag = False
        Subject = "[taint]"+fake.sentence(nb_words=20, variable_nb_words=True)
        SubjectTag = True # if a email is read as well as draft, it does not exist actually, so the tag is false.
        UniqueBody = "[taint]"+fake.paragraph(nb_sentences=10, variable_nb_sentences=True)
        UniqueBodyTag = True # if a email is read as well as draft, it does not exist actually, so the tag is false.
        SentDateTime = fake.date_time_between(start_date="-10y")
        SentDateTimeTag = False
        Puser = "[taint]"+sender[2]
        PuserTag = True # GUID is always sensitive
    else:
        Sender = sender[0]
        SenderTag = False
        SenderAddress = sender[1]
        SenderAddressTag = False # email address is considered sensitive
        Receiver = receiver[0]
        ReceiverTag = False
        ReceiverAddress = receiver[1]
        ReceiverAddressTag = False # email address is considered sensitive
        IsRead = fake.pybool()
        IsReadTag = False
        IsDraft = fake.pybool()
        IsDraftTag = False
        Subject = ""
        SubjectTag = False # if a email is read as well as draft, it does not exist actually, so the tag is false.
        UniqueBody = ""
        UniqueBodyTag = False # if a email is read as well as draft, it does not exist actually, so the tag is false.
        SentDateTime = fake.date_time_between(start_date="-10y")
        SentDateTimeTag = False
        Puser = ""
        PuserTag = False # GUID is always sensitive

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
        