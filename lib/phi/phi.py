import astroid
from lib.phi.phi_stmt import phi_stmt
from lib.phi.phi_mapreduce import phi_mapreduce
from lib.phi.util import get_func_name
from lib.phi.constant import RewriteError, ExecutionError, TaintStreamError
from lib.taint_dataframe import cal_table_tag, untaint, taint_table
from lib.dependency.dependency_graph import add_dependency

import time

code2new_code = []

input_apis = [
    'read.json', 
    'read.parquet', 
    'read.format("csv")', 
    "read.format('csv')", 
    'read.format("tsv")', 
    "read.format('tsv')",
    'load_curated_dataset',
]

input_apis_meta = {
    'read.json': 0, # first arg is the input path
    'read.parquet': 0, 
    'read.format("csv")': 0, 
    "read.format('csv')": 0, 
    'read.format("tsv")': 0, 
    "read.format('tsv')": 0,
    'load_curated_dataset': 3,
}

structured_apis = [
    'select', 
    'drop',
    'withColumn',
    'orderBy', 
    'union', 
    'filter', 
    'where', 
    'join', 
    'agg',
    'distinct',
    'count',
    'collect',
]


def get_root_df(node):
    while isinstance(node, astroid.nodes.Call) and isinstance(node.func, astroid.nodes.Attribute):
        node = node.func.expr
        if isinstance(node, astroid.nodes.Attribute):
            node = node.expr
    return node.as_string()


def seems_like_input_api(func_code):
    for input_api in input_apis:
        if input_api in func_code:
            return True


def phi(code, globs, locs):
    try:
        tree = astroid.extract_node(code)
    except ValueError:
        return code, ''
    if isinstance(tree, astroid.nodes.Assign) and len(tree.targets) == 1:
        value = tree.value
        if isinstance(value, astroid.nodes.Call):
            func = value.func
            if seems_like_input_api(func.as_string()):
                return phi_input(code, globs, locs)
            func_name = get_func_name(value)
            if func_name in structured_apis:
                # get df
                df = get_root_df(value)
                code, export_code = phi_stmt(value.as_string(), df, globs, locs)
                tree.value = astroid.extract_node(code)
                return tree.as_string(), export_code
            if 'reduceByKey' in code and 'map' in code and func.attrname == 'toDF':
                df = get_root_df(value)
                code, export_code = phi_mapreduce(value.as_string(), df, globs, locs)
                tree.value = astroid.extract_node(code)
                return tree.as_string(), export_code
    return code, ''

def phi_input(code, globs, locs):
    export_code = f"""from lib.taint_dataframe import taint_cols
from lib.phi.constant import taint_col_handler
"""
    tree = astroid.extract_node(code)
    value_code = tree.value.as_string()
    new_value_code = f"taint_cols({value_code}, taint_col_handler.get_tainted_cols())"
    value_node = astroid.extract_node(new_value_code)
    tree.value = value_node
    return tree.as_string(), export_code


def rewrite_and_execute(code, context):
    try:
        globs, locs = context
        if "map" in code and "reduceByKey" in code:
            globs.update(locs) # this step is needed for map reduce
        add_dependency(code, globs, locs) # maintain dependency graph 
        st = time.time()
        new_code, export_code = phi(code, globs, locs) # dynamic code rewrite
        ed = time.time()
        code2new_code.append({code: {"new_code": new_code, "export_code": export_code}})
        fp = open("code_mapping.json", "w")
        import json
        json.dump({"mapping": code2new_code}, fp, indent=2)
        print(f"dynamic code rewrite overhead: {ed-st}")
        try:
            exec(export_code, globs, locs)
            globs.update(locs)
            exec(new_code, globs, locs)
            # get target variable name
            tree = astroid.extract_node(code)
            target = tree.targets[0].name
            return locs[target]
        except Exception:
            print("error occured when running rewritten code")
            print(f"original code: {code}")
            # print("export code:\n", export_code)
            print(f"rewritten code: {new_code}")
            import traceback
            traceback.print_exc()
            raise ExecutionError
    except TaintStreamError as e:
        if isinstance(e, RewriteError):
            # print(f"reason: {e.message}")
            import traceback
            traceback.print_exc()
            print(f"TaintStream fails to rewrite code: {code}")
            # print("export code:\n", export_code)
            # print("rewritten code:\n", new_code)
        elif isinstance(e, ExecutionError):
            pass
        else:
            import traceback
            traceback.print_exc()
        print("[WARN] TaintStream loses fine-grained tracking here.")
        
        # get target variable name
        tree = astroid.extract_node(code)
        target = tree.targets[0].name

        # get dft (dataframe_taint)
        globs, locs = context
        root_df_code = get_root_df(tree.value)
        dft = eval(root_df_code, globs, locs)
        tag = cal_table_tag(dft)
        df = untaint(dft)
        locs[root_df_code] = df
        
        exec(code, globs, locs)
        
        df = locs[target]
        return taint_table(df, tag)



def extract_dft(code, context):
    globs, locs = context
    tree = astroid.extract_node(code)
    df_code = get_root_df(tree.value)
    return eval(df_code, globs, locs)