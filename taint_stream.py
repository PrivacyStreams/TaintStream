# transform a script to make it support taint tracking

import inspect
import ast
import os
import traceback
import json
import argparse
from transformer.transform import transform

default_wockdir = "example/"
default_output = "placeholder"

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-wd', '--work_dir', type=str, dest="work_dir", help='path of the work dir', required=False, default=default_wockdir)
    parser.add_argument('-i', '--input', type=str, dest="input", help='input file\'s path related to work dir', required=True)
    parser.add_argument('-o', '--output', type=str, dest="output", default=default_output, help="output file's name, the output file will be put in the work dir")
    parser.add_argument('-t', "--taint", type=str, dest="taint_cols", nargs='+', help='columns to taint', required=False)
    parser.add_argument('-a', "--args", type=str, dest="args", nargs='+', help='arguments of the input script', required=False)
    parser.add_argument('-c', "--config", type=str, dest="config", default="./config_template.json", help='config file of the TaintStream, which contains customized hooked method', required=False)

    args = parser.parse_args()

    try:
        input_f = open(f"{args.work_dir}/{args.input}", 'r')
        code = input_f.read()
        import time
        time_start=time.time()
        transformed_code = transform(code, args.taint_cols, args.config)
        time_end=time.time()
        print('code-rewrite cost',time_end-time_start)
        input_f.close()
        if args.output == default_output:
            if "/" in args.input:
                input_file_name = args.input.split("/")[-1]
            else:
                input_file_name = args.input
            output = "transformed_" + input_file_name
        else:
            output = args.output if args.output[-3:] == '.py' else args.output+'.py'
        output_f = open(output, 'w')
        output_f.write(transformed_code)
        print(f"transformed scripts saved as {output}")
        output_f.close()
    except Exception as e:
        traceback.print_exc()
        raise e
    os.system(f'cp -r lib {args.work_dir}/')
    os.system(f'mv {output} {args.work_dir}/{output}')
    os.chdir(f"{args.work_dir}")
    os.system(f'spark-submit --jars lib/taint_stream_lib.jar {output}')

if __name__ == "__main__":
    main()