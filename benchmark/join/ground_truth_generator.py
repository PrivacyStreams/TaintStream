import json
import os

output_dir = "./ground_truth"

def inner_join_1():
    res = {
        "Sender": False,
        "Receiver": False,
        "EmailAddress": True,
        "PhoneNumber": True,
    }
    f = open(f"{output_dir}/inner_join_1.json", "w")
    print("writing ground truth of inner_join_1")
    json.dump(res, f, indent=2)
    f.close()


def inner_join_2():
    res = {
        "SenderEmailAddress": True,
        "SenderJob": False,
        "ReceiverEmailAddress": True,
        "ReceiverJob": False,
        "Subject": True,
    }
    f = open(f"{output_dir}/inner_join_2.json", "w")
    print("writing ground truth of inner_join_2")
    json.dump(res, f, indent=2)
    f.close()


def inner_join_3():
    res = {
        "SenderEmailAddress": True,
        "SenderJob": False,
        "ReceiverEmailAddress": True,
        "ReceiverJob": False,
        "email_count": False,
    }
    f = open(f"{output_dir}/inner_join_3.json", "w")
    print("writing ground truth of inner_join_3")
    json.dump(res, f, indent=2)
    f.close()


def left_join():
    res = {
        "SenderEmailAddress": True,
        "SenderJob": False,
        "ReceiverEmailAddress": True,
        "ReceiverJob": False,
        "Subject": True,
    }
    f = open(f"{output_dir}/left_join.json", "w")
    print("writing ground truth of left_join")
    json.dump(res, f, indent=2)
    f.close()


def right_join():
    res = {
        "SenderEmailAddress": True,
        "SenderJob": False,
        "ReceiverEmailAddress": True,
        "ReceiverJob": False,
        "Subject": True,
    }
    f = open(f"{output_dir}/right_join.json", "w")
    print("writing ground truth of right_join")
    json.dump(res, f, indent=2)
    f.close()


def outer_join():
    res = {
        "SenderEmailAddress": True,
        "SenderJob": False,
        "ReceiverEmailAddress": True,
        "ReceiverJob": False,
        "Subject": True,
    }
    f = open(f"{output_dir}/outer_join.json", "w")
    print("writing ground truth of outer_join")
    json.dump(res, f, indent=2)
    f.close()



def main():
    inner_join_1()
    inner_join_2()
    inner_join_3()
    left_join()
    right_join()
    outer_join()




if __name__ == "__main__":
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    main()