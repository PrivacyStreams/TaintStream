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

outlier_rate = 0.1 # proportion of the perple that are not covered by person table


email_output_dir = "./fake_email_data"
person_output_dir = "./fake_person_data"

tagged_email_output_dir = "./fake_tagged_email_data"
tagged_person_output_dir = "./fake_tagged_person_data"



class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj,datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return json.JSONEncoder.default(self,obj)

def new_row_email():
    row = {}
    tagged_row = {}
    
    fields = [
        "Sender",
        "Receiver",
        "Subject",
        "UniqueBody",
        "SentDateTime",
        "Email_ID",
        "IsRead",
        "IsDraft"
    ]

    sender = user_list[random.randint(0, user_num-1)]
    receiver = user_list[random.randint(0, user_num-1)]
    if random.random() > 0.5:
        Sender = sender[0]
        SenderTag = False
        Receiver = receiver[0]
        ReceiverTag = False
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
        Email_ID = "[taint]"+fake.md5()
        Email_IDTag = True # Email_ID ID is always sensitive
    else:
        Sender = sender[0]
        SenderTag = False
        Receiver = receiver[0]
        ReceiverTag = False
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
        Email_ID = ""
        Email_IDTag = False # Email_ID ID is always sensitive

    locs = locals().copy()
    for field in fields:
        row[field] = locs[field]
        tagged_row[field] = {"value":locs[field], "tag": locs[field+"Tag"]}

    return row, tagged_row


def new_row_person(i):
    row = {}
    tagged_row = {}

    fields = [
        "Name",
        "EmailAddress",
        "FirstName",
        "LastName",
        "PhoneNumber",
        "Job",
        "GUID",
        "Company",
        "Age"
    ]

    person = user_list[i]
    import random
    if random.random() > 0.5:
        Name = person[0]
        NameTag = False
        EmailAddress = "[taint]"+person[1]
        EmailAddressTag = True
        FirstName = fake.first_name()
        FirstNameTag = False
        LastName = fake.last_name()
        LastNameTag = False
        PhoneNumber = "[taint]"+fake.phone_number()
        PhoneNumberTag = True
        Job = fake.job()
        JobTag = False
        GUID = "[taint]"+person[2]
        GUIDTag = True
        Company = fake.company()
        CompanyTag = False
        Age = fake.pyint(min_value=0, max_value=99)
        AgeTag = False
    else:
        Name = person[0]
        NameTag = False
        EmailAddress = ""
        EmailAddressTag = False
        FirstName = fake.first_name()
        FirstNameTag = False
        LastName = fake.last_name()
        LastNameTag = False
        PhoneNumber = ""
        PhoneNumberTag = False
        Job = fake.job()
        JobTag = False
        GUID = ""
        GUIDTag = False
        Company = fake.company()
        CompanyTag = False
        Age = fake.pyint(min_value=0, max_value=99)
        AgeTag = False

    locs = locals().copy()
    for field in fields:
        row[field] = locs[field]
        tagged_row[field] = {"value":locs[field], "tag": locs[field+"Tag"]}
    
    import random
    sample = random.random()
    if sample < outlier_rate:
        Name = fake.name()
        row["Name"] = Name
        tagged_row["Name"]["value"] = Name

    return row, tagged_row

if __name__ == "__main__":
    if not os.path.exists(email_output_dir):
        os.mkdir(email_output_dir)
    
    if not os.path.exists(person_output_dir):
        os.mkdir(person_output_dir)

    if not os.path.exists(tagged_email_output_dir):
        os.mkdir(tagged_email_output_dir)
    
    if not os.path.exists(tagged_person_output_dir):
        os.mkdir(tagged_person_output_dir)
    
    # email data
    print("generating email data")
    file_no = 0
    file_no_str = format(file_no, "05d")
    f = open(f"{email_output_dir}/{file_no_str}.json", "w")
    tf = open(f"{tagged_email_output_dir}/{file_no_str}.json", "w")
    for tot in range(row_num):
        row, tagged_row = new_row_email()
        row_str = json.dumps(row, cls=DateEncoder)
        tagged_row_str = json.dumps(tagged_row, cls=DateEncoder)
        f.write(row_str + "\n")
        tf.write(tagged_row_str + "\n")
        if (tot+1) % save_every == 0:
            f.close()
            file_no += 1
            file_no_str = format(file_no, "05d")
            f = open(f"{email_output_dir}/{file_no_str}.json", "w")
            tf = open(f"{tagged_email_output_dir}/{file_no_str}.json", "w")
            print(f"generate {tot+1} rows...")
    if not f.closed:
        f.close()
        print(f"generate {row_num} rows...")
    if not tf.closed:
        tf.close()
        print(f"generate {row_num} rows...")
    

    # person data
    print("generating person data")
    file_no = 0
    file_no_str = format(file_no, "05d")
    f = open(f"{person_output_dir}/{file_no_str}.json", "w")
    tf = open(f"{tagged_person_output_dir}/{file_no_str}.json", "w")
    for tot in range(user_num):
        row, tagged_row = new_row_person(tot)
        row_str = json.dumps(row, cls=DateEncoder)
        tagged_row_str = json.dumps(tagged_row, cls=DateEncoder)
        f.write(row_str + "\n")
        tf.write(tagged_row_str + "\n")
        if (tot+1) % save_every == 0:
            f.close()
            file_no += 1
            file_no_str = format(file_no, "05d")
            f = open(f"{person_output_dir}/{file_no_str}.json", "w")
            tf = open(f"{tagged_person_output_dir}/{file_no_str}.json", "w")
            print(f"generate {tot+1} rows...")
    if not f.closed:
        f.close()
        print(f"generate {user_num} rows...")
    if not tf.closed:
        tf.close()
        print(f"generate {row_num} rows...")