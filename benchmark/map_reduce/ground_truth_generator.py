import json
import os

output_dir = "./ground_truth"

def map_reduce_1():
    res = {
        "Sender": True,
        "Email_Count": False,
    }
    f = open(f"{output_dir}/map_reduce_1.json", "w")
    print("writing ground truth of map_reduce_1")
    json.dump(res, f, indent=2)
    f.close()


def map_reduce_2():
    res = {
        "Sender": True,
        "Receiver": False,
        "SubjectList": True,
        "EmailCount": False,
    }
    f = open(f"{output_dir}/map_reduce_2.json", "w")
    print("writing ground truth of map_reduce_2")
    json.dump(res, f, indent=2)
    f.close()


def map_reduce_3():
    res = {
        "Sender": True,
        "Receiver": False,
        "SubjectList": True,
        "EmailCount": False,
    }
    f = open(f"{output_dir}/map_reduce_3.json", "w")
    print("writing ground truth of map_reduce_3")
    json.dump(res, f, indent=2)
    f.close()


def map_reduce_4():
    res = {
        "Sender": True,
        "Receiver": False,
        "SubjectList": True,
        "EmailCount": False,
    }
    f = open(f"{output_dir}/map_reduce_4.json", "w")
    print("writing ground truth of map_reduce_4")
    json.dump(res, f, indent=2)
    f.close()



def main():
    map_reduce_1()
    map_reduce_2()
    map_reduce_3()
    map_reduce_4()




if __name__ == "__main__":
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    main()