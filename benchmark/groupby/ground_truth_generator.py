import json
import os

output_dir = "./ground_truth"

def count_1():
    res = {
        "SenderAddress": True,
        "email_count": False,
    }
    f = open(f"{output_dir}/count_1.json", "w")
    print("writing ground truth of count_1")
    json.dump(res, f, indent=2)
    f.close()


def count_2():
    res = {
        "Sender": False,
        "SenderAddress": True,
        "number of send to": False,
    }
    f = open(f"{output_dir}/count_2.json", "w")
    print("writing ground truth of count_2")
    json.dump(res, f, indent=2)
    f.close()


def count_3():
    res = {
        "Sender": False,
        "Receiver": False,
        "count": True,
        "SentDateTime": False,
    }
    f = open(f"{output_dir}/count_3.json", "w")
    print("writing ground truth of count_3")
    json.dump(res, f, indent=2)
    f.close()


def count_4():
    res = {
        "Puser": True,
        "sender_num": False,
        "receiver_num": False,
        "subject_num": True,
    }
    f = open(f"{output_dir}/count_4.json", "w")
    print("writing ground truth of count_4")
    json.dump(res, f, indent=2)
    f.close()


def statistics_1():
    res = {
        "year": False,
        "sum of body_length": True,
        # "min of body_length": True,
        "max of body_length": True,
        "avg of body_length": True,
        # "sd": True,
    }
    f = open(f"{output_dir}/statistics_1.json", "w")
    print("writing ground truth of statistics_1")
    json.dump(res, f, indent=2)
    f.close()


def statistics_2():
    res = {
        "stddev_pop(Float)": True,
        "IntSum": True,
        "IntAvg": True,
        "ShortSum": False,
        "ShortAvg": False,
    }
    f = open(f"{output_dir}/statistics_2.json", "w")
    print("writing ground truth of statistics_2")
    json.dump(res, f, indent=2)
    f.close()


def main():
    count_1()
    count_2()
    count_3()
    count_4()
    statistics_1()
    statistics_2()




if __name__ == "__main__":
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    main()