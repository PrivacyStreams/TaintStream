# transform a script to make it support taint tracking

import inspect
import traceback
import astroid
import json
from transformer.format_transformer import reformate
# from transformer import *

# list of df in code
seems_like_df = set()

def need_instrument(node):
    if not isinstance(node, astroid.nodes.Assign):
        return False
    if len(node.targets) > 1:
        return False
    from lib.phi.phi import seems_like_input_api, structured_apis, get_root_df
    from lib.phi.util import get_func_name
    value = node.value
    if isinstance(value, astroid.nodes.Call):
        func = value.func
        root_df = get_root_df(value)
        target = node.targets[0].as_string()
        # input
        if seems_like_input_api(func.as_string()):
            seems_like_df.add(target)
            return True
        func_name = get_func_name(value)
        # structured api
        if func_name in structured_apis:
            # special pricess for join to avoid mis-rewritting
            if func_name == 'join':
                if root_df in seems_like_df:
                    seems_like_df.add(target)
                    seems_like_df.add(root_df)
                    return True
                else:
                    return False
            else:
                seems_like_df.add(target)
                seems_like_df.add(root_df)
                return True
        code = node.as_string()
        # map reduce
        if 'reduceByKey' in code and 'map' in code and func_name == 'toDF':
            seems_like_df.add(target)
            seems_like_df.add(root_df)
            return True
    return False


def transform_body(node_list):
    if node_list is None:
        return None
    assert isinstance(node_list, list) 
    ret_list = []
    for node in node_list:
        if need_instrument(node):
            # follow https://www.sololearn.com/Discuss/1336429/exec-doesn-t-work-with-local-variables-in-function-python to work with local variables
            ret_list.append(build_get_context_node())
            ret_list.append(build_code_node(node))
            ret_list.append(build_run_and_execute_node(node))
            """
            ret_list.append(build_temp_locs_node())
            ret_list.append(build_code_node(node))
            ret_list.append(build_phi_node())
            ret_list.append(build_run_export_node())
            ret_list.append(build_exec_node())
            ret_list.append(build_assgin_loc_node(node))
            """
        else:
            ret_list.append(node)
    return ret_list


def build_get_context_node():
    return astroid.extract_node("context = globals().copy(), locals().copy()")


def build_run_and_execute_node(node):
    node.value = astroid.extract_node("rewrite_and_execute(code, context)")
    return node


def build_temp_locs_node():
    return astroid.extract_node("temp_locs=locals()")


def build_code_node(node):
    node_code = node.as_string()
    ret_node = astroid.nodes.Assign()
    ret_node.targets = [astroid.nodes.Name("code")]
    ret_node.value = astroid.nodes.Const(node_code)
    return ret_node

def build_phi_node():
    return astroid.extract_node("new_code, export_code = phi(code, globals(), temp_locs)")


def build_run_export_node():
    return astroid.extract_node("exec(export_code)")


def build_exec_node():
    return astroid.extract_node("exec(new_code, globals(), temp_locs)")


def build_assgin_loc_node(node):
    target = node.targets[0]
    assert isinstance(target, astroid.nodes.AssignName) # ant other cases?
    target_code = node.targets[0].as_string().strip()
    ret_node = astroid.nodes.Assign()
    ret_node.targets = [astroid.nodes.Name(target_code)]
    value_node = astroid.extract_node(f"temp_locs['1']")
    # change slice node
    slice_node = astroid.nodes.Index()
    slice_node.postinit(value=astroid.nodes.Const(value=target_code))
    value_node.slice = slice_node
    ret_node.value = value_node
    return ret_node


def multilineblock_transform(block):
    body = block.body
    if not isinstance(body, list):
        print(block.parent.parent.parent.as_string())
    block.body = transform_body(block.body)
    if hasattr(block, "orelse"):
        block.orelse = transform_body(block.orelse)
    return block
        

def seems_like_custom_multilineblock(node):
    return hasattr(node, "body")


def user_customized_hook(code: str, config):
    if config is None:
        return code
    code = config["insert_code"] + code
    for key in config["customized_hook"]:
        code = code.replace(key, config["customized_hook"][key])
    return code

def transform(code, taint_cols, config_path):
    # insert TaintStream lib
    
    try:
        f = open(config_path, "r")
        config = json.load(f)
    except:
        config = None

    code = user_customized_hook(code, config)

    code = reformate(code)

    # rewrite code
    registerd_nodes = []
    for node_str in dir(astroid.nodes):
        if "__" in node_str or node_str=='IfExp' or node_str=='Lambda':
            continue
        node = astroid.nodes.__getattribute__(node_str)
        try:
            fields = node.__getattribute__(node, '_astroid_fields')
        except:
            continue
        if "body" in fields:
            astroid.MANAGER.register_transform(
                node,
                multilineblock_transform,
                seems_like_custom_multilineblock
            )
            registerd_nodes.append(node)
    # print("register node to transform:")
    # for node in registerd_nodes:
    #     print(node) 
    tree = astroid.parse(code)
    for node in registerd_nodes:
        astroid.MANAGER.unregister_transform(
                node,
                multilineblock_transform,
                seems_like_custom_multilineblock
            )
    code = tree.as_string()

    code_to_insert = f"""from lib.phi.phi import rewrite_and_execute
from lib.phi.constant import taint_col_handler
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *
from lib.udf import *
from lib.dependency.dependency_graph import save_graph
"""
    if taint_cols is not None:
        for taint_col in taint_cols:
            code_to_insert += f"taint_col_handler.add_tainted_col('{taint_col}')\n"
    code = code_to_insert + code

    code_to_insert = "\nsave_graph()"
    code = code + code_to_insert

    return code



