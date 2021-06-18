import json
import os

output_dir = "./ground_truth"

def array_type_1():
    res = {
        "Sender": False,
        "CcSet": True,
        "CcList": True,
        "CcSetLength": True,
        "CcListLength": True,
    }
    f = open(f"{output_dir}/array_type_1.json", "w")
    print("writing ground truth of array_type_1")
    json.dump(res, f, indent=2)
    f.close()


def array_type_2():
    res = {
        "Sender": False,
        "1st_receiver": True,
        "5th_receiver": True,
    }
    f = open(f"{output_dir}/array_type_2.json", "w")
    print("writing ground truth of array_type_2")
    json.dump(res, f, indent=2)
    f.close()


def structured_type_1():
    res = {
        # "Content": True,
        "Content.Subject": False,
        "Content.UniqueBody": True,
        "Subject_word_count": False,
        "UniqueBody_word_count": False,
    }
    f = open(f"{output_dir}/structured_type_1.json", "w")
    print("writing ground truth of structured_type_1")
    json.dump(res, f, indent=2)
    f.close()


def structured_type_2():
    res = {
        "Sender": False,
        "avg_content_length": True,
        "Profile": False,
    }
    f = open(f"{output_dir}/structured_type_2.json", "w")
    print("writing ground truth of structured_type_2")
    json.dump(res, f, indent=2)
    f.close()


def structured_type_3():
    res = {
        "Sender": False,
        "content_length": True,
        "address": True,
        "Profile": True
    }
    f = open(f"{output_dir}/structured_type_3.json", "w")
    print("writing ground truth of structured_type_3")
    json.dump(res, f, indent=2)
    f.close()

def main():
    array_type_1()
    array_type_2()
    structured_type_1()
    structured_type_2()
    structured_type_3()




if __name__ == "__main__":
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    main()