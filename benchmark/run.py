import os
import time
import subprocess
from tempfile import NamedTemporaryFile
import json

original_run_num = 1
taint_stream_run_num = 1

categories = [
    "basic",
    "groupby",
    "join",
    "structured_data",
    "udf",
    "map_reduce"
]

basic_scripts = [
    "orderBy_1.py",
    "orderBy_2.py",
    "select_and_filter_1.py",
    "select_and_filter_2.py",
    "withColumn_1.py",
    "withColumn_2.py",
    "withColumn_3.py",
]

groupby_scripts = [
    "count_1.py",
    "count_2.py",
    "count_3.py",
    "count_4.py",
    "statistics_1.py",
    "statistics_2.py",
]

join_scripts = [
    "inner_join_1.py",
    "inner_join_2.py",
    "inner_join_3.py",
    "left_join.py",
    "right_join.py",
    "outer_join.py",
]

udf_scripts = [
    "udf_1.py",
    "udf_2.py",
    "udf_3.py",
    "class_udf_1.py",
    "class_udf_2.py",
]

structured_data_scripts = [
    "array_type_1.py",
    "array_type_2.py",
    "structured_type_1.py",
    "structured_type_2.py",
    "structured_type_3.py",
]

map_reduce_scripts = [
    "map_reduce_1.py",
    "map_reduce_2.py",
    "map_reduce_3.py",
    "map_reduce_4.py"
]

def cprint(msg):
    print("[benchmark info]: ", msg)


def get_running_time(log):
    t = -1
    log = log.decode()
    lines = log.split('\n')
    for line in lines:
        if "[benchmark info]" in line:
            try:
                t = float(line.split()[-1][:-1])
            except:
                pass
    return t


def get_dynamic_code_rewrite_overhead(log):
    res = 0
    log = log.decode()
    lines = log.split('\n')
    for line in lines:
        if "dynamic code rewrite overhead" in line:
            try:
                res += float(line.split()[-1])
            except:
                pass
    return res



def get_rewriting_time(log):
    t = -1
    log = log.decode()
    lines = log.split('\n')
    for line in lines:
        if "code-rewrite cost" in line:
            try:
                t = float(line.split()[-1])
            except:
                pass
    return t

def test_category(category):
    # print(os.path.abspath(os.curdir))
    if category not in categories:
        cprint(f"category ({category}) does not exist, available categories: {categories}")
        return
    if os.path.exists(f"{category}/log.txt"):
        os.remove(f"{category}/log.txt")
    if "complex" in category:
        scripts = [ category[len("complex/"):] + ".py" ]
    else:
        scripts = globals()[f"{category}_scripts"]
    
    # generate ground truth
    os.chdir(f"benchmark_cell/{category}/")
    os.system("python ground_truth_generator.py")
    os.chdir("../../")
    
    for s in scripts:
        # original
        os.chdir(f"benchmark_cell/{category}/")
        running_time_list = []
        cprint(f"original script: {s}")
        for i in range(original_run_num):
            with NamedTemporaryFile() as f:
                subprocess.call(
                    ["python", s],
                    stdout=f,
                    stderr=f
                )
                f.seek(0)
                output = f.read()
                t = get_running_time(output)
                if t == -1:
                    cprint(f"{s} crashed!")
                    print(output.decode())
                    break
            running_time_list.append(t)
            # cprint(f"original running cost of {s} ({i}th time): {t}s")
        if original_run_num > 0:
            cprint(f"averaged original running cost: {sum(running_time_list)/len(running_time_list)}s")


        # taint_stream
        os.chdir("../../")
        if "complex" in category:
            os.chdir("..")
        running_time_list = []
        rewriting_time_list = []
        dynamic_rewrite_code_time_list = []
        cprint(f"testing TaintStream on {s}")
        for i in range(taint_stream_run_num):
            with NamedTemporaryFile() as f:
                subprocess.call(
                    ["python", "taint_stream.py", "-wd", f"./benchmark_cell/{category}/", "-i", s, "-c", f"./benchmark_cell/{category}/{category}_config.json"],
                    stdout=f,
                    stderr=f
                )
                f.seek(0)
                output = f.read()
                running_time = get_running_time(output)
                rewriting_time = get_rewriting_time(output)
                dynamic_rewrite_code_time = get_dynamic_code_rewrite_overhead(output)
            if running_time == -1:
                cprint(f"{s} crashed!")
                print(output.decode())
                break
            running_time_list.append(running_time)
            rewriting_time_list.append(rewriting_time)
            dynamic_rewrite_code_time_list.append(dynamic_rewrite_code_time)
            # cprint(f"taint_stream running cost of {s} ({i}th time): {running_time}s")
            # cprint(f"taint_stream rewritting cost of {s} ({i}th time): {rewriting_time}s")
            # cprint(f"taint_stream dynamic rewritting cost of {s} ({i}th time): {dynamic_rewrite_code_time}s")
        if taint_stream_run_num > 0:
            avg_run = sum(running_time_list)/len(running_time_list)
            avg_rewriting = sum(rewriting_time_list)/len(rewriting_time_list)
            avg_dynamic_rewriting = sum(dynamic_rewrite_code_time_list)/len(dynamic_rewrite_code_time_list)
            cprint(f"averaged TaintStream running cost: {avg_run}s")
            cprint(f"averaged TaintStream rewriting cost: {avg_rewriting}s")
            cprint(f"averaged TaintStream dynamic rewriting cost: {avg_dynamic_rewriting}s")
            cprint(f"averaged TaintStream total cost: {avg_run + avg_rewriting}s")

        # check results
        output_dir = f"./benchmark_cell/{category}/transformed_{s[:-3]}_output"
        files = os.listdir(output_dir)
        for f in files:
            if f[-4:] == "json":
                output_file = f"{output_dir}/{f}"
        ground_truth_f = open(f"./benchmark_cell/{category}/ground_truth/{s[:-3]}.json", "r")
        ground_truth = json.load(ground_truth_f)
        ground_truth_f.close()
        ret = check(ground_truth, output_file)
        cprint(f"{s} checking results [TP, TN, FP, FN]: {ret}")
        # cprint(f"precision: {ret[0]/(ret[0]+ret[2])}, recall: {ret[0]/(ret[0]+ret[3])}")
        
def check(ground_truth, result_file):
    ret = [0,0,0,0] # TP, TN, FP, FN
    results = open(result_file, "r")
    test_cases = list(ground_truth.keys()) 
    cprint(f"test cases: {test_cases}")
    for line in results:
        row = json.loads(line)    
        for tc in test_cases:
            negative_flag = False
            if ground_truth[tc] == False:
                negative_flag = True
            try:
                if "." in tc: # structured data
                    tc_1, tc_2 = tc.split(".")
                    value = row[tc_1][tc_2]["value"]
                    tag = row[tc_1][tc_2]["tag"]
                else:
                    value = row[tc]["value"]
                    tag = row[tc]["tag"]
                cell_result = cell_checker(value, tag, negative_flag)
                ret[cell_result] += 1
            except:
                import traceback
                traceback.print_exc()
    return ret

# return 0/1/2/3 for TP, TN, FP, FN respectively 
def cell_checker(value, tag, negative_flag):
    g_tag = False
    if isinstance(value, bool):
        return 1 # TN
    elif isinstance(value, str):
        g_tag = "[taint]" in value
    elif isinstance(value, int):
        g_tag = value != 0
    elif isinstance(value, float):
        g_tag = abs((value-0)) > 1e-20
    elif isinstance(value, list):
        for item in value:
            if "[taint]" in item:
                g_tag = True
                break
    else:
        cprint(f"unknown type: {value}")
        assert False
    if negative_flag:
        g_tag = False 
    if tag and g_tag:
        return 0 # TP
    elif (not tag) and (not g_tag):
        return 1 # TN
    elif tag and (not g_tag):
        return 2 # FP
    elif (not tag) and g_tag:
        # print(value, tag)
        return 3 # FN

def main():
    os.chdir("..")
    for c in categories:
        cprint(f"======= {c} testing =======")
        test_category(c)

if __name__ == "__main__":
    main()
