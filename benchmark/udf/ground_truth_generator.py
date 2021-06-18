import json
import os

output_dir = "./ground_truth"

def udf_1():
    res = {
        "lit_udf": False,
        "one_udf": False,
        "two_udf": False,
        "three_udf": True
    }
    f = open(f"{output_dir}/udf_1.json", "w")
    print("writing ground truth of udf_1s")
    json.dump(res, f, indent=2)
    f.close()


def udf_2():
    res = {
        "word_count": True,
    }
    f = open(f"{output_dir}/udf_2.json", "w")
    print("writing ground truth of udf_2")
    json.dump(res, f, indent=2)
    f.close()


def udf_3():
    res = {
        "sender_only": False,
        "Email_ID": True,
        "UniqueBody": True,
    }
    f = open(f"{output_dir}/udf_3.json", "w")
    print("writing ground truth of udf_3")
    json.dump(res, f, indent=2)
    f.close()


def class_udf_1():
    res = {
        "UniqueBody": True,
        "words1": True,
    }
    f = open(f"{output_dir}/class_udf_1.json", "w")
    print("writing ground truth of class_udf_1")
    json.dump(res, f, indent=2)
    f.close()


def class_udf_2():
    res = {
        "intent_response_json": True,
    }
    f = open(f"{output_dir}/class_udf_2.json", "w")
    print("writing ground truth of class_udf_2")
    json.dump(res, f, indent=2)
    f.close()


def main():
    udf_1()
    udf_2()
    udf_3()
    class_udf_1()
    class_udf_2()




if __name__ == "__main__":
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    main()