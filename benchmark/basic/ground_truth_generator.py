import json
import os

output_dir = "./ground_truth"

def orderBy_1():
    res = {
        "Sender": False,
        "SenderAddress": True,
        "Receiver": False,
        "ReceiverAddress": True,
        "Puser": True,
    }
    f = open(f"{output_dir}/orderBy_1.json", "w")
    print("writing ground truth of orderBy_1")
    json.dump(res, f, indent=2)
    f.close()


def orderBy_2():
    res = {
        "year": False,
        "month": False,
        "UniqueBody": True
    }
    f = open(f"{output_dir}/orderBy_2.json", "w")
    print("writing ground truth of orderBy_2")
    json.dump(res, f, indent=2)
    f.close()


def select_and_filter_1():
    res = {
        "Subject": True,
        "UniqueBody": True,
        "IsRead": False,
        "IsDraft": False,
        "SenderType": False,
        "fussion": True,
    }
    f = open(f"{output_dir}/select_and_filter_1.json", "w")
    print("writing ground truth of select_and_filter_1")
    json.dump(res, f, indent=2)
    f.close()


def select_and_filter_2():
    res = {
        "Sender": False,
        "UniqueBody": True,
    }
    f = open(f"{output_dir}/select_and_filter_2.json", "w")
    print("writing ground truth of select_and_filter_2")
    json.dump(res, f, indent=2)
    f.close()


def withColumn_1():
    res = {
        "FirstSentence": True,
    }
    f = open(f"{output_dir}/withColumn_1.json", "w")
    print("writing ground truth of withColumn_1")
    json.dump(res, f, indent=2)
    f.close()


def withColumn_2():
    res = {
        "Byte+Short": True,
        "Int*Long": True,
        "Float/Double": True,
        "BinaryDecode": False
    }
    f = open(f"{output_dir}/withColumn_2.json", "w")
    print("writing ground truth of withColumn_2")
    json.dump(res, f, indent=2)
    f.close()


def withColumn_3():
    res = {
        "concat_sender": True,
        "concat_receiver": True,
        "concat_subject_and_body": True
    }
    f = open(f"{output_dir}/withColumn_3.json", "w")
    print("writing ground truth of withColumn_3")
    json.dump(res, f, indent=2)
    f.close()


def main():
    orderBy_1()
    orderBy_2()
    select_and_filter_1()
    select_and_filter_2()
    withColumn_1()
    withColumn_2()
    withColumn_3()




if __name__ == "__main__":
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    main()