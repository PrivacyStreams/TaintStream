import astroid
from lib.phi.util import *
from lib.phi.phi_col import *


def analyze_dependency(tree, globs, locs):
    if isinstance(tree, astroid.nodes.Const):
        return analyze_const(tree, globs, locs)
    elif isinstance(tree, astroid.nodes.Name):
        return analyze_name(tree, globs, locs)
    elif isinstance(tree, astroid.nodes.Call):
        return analyze_call(tree, globs, locs)
    elif isinstance(tree, astroid.nodes.BinOp):
        return analyze_binop(tree, globs, locs)
    elif isinstance(tree, astroid.nodes.Compare):
        return analyze_comp(tree, globs, locs)
    elif isinstance(tree, astroid.nodes.Attribute):
        return analyze_attr(tree, globs, locs)
    elif isinstance(tree, astroid.nodes.Subscript):
        return analyze_subscript(tree, globs, locs)


def analyze_const(tree, globs, locs):
    ret = []
    if isinstance(tree.value, str): # string type has two case
        if tree.value == "''" or tree.value == '""':
            ret.append('lit')
        else:
            ret.append(f'${tree.value}')
    else: # other types are considered as lit
        ret.append('lit')
    return ret


def analyze_name(tree, globs, locs):
    ret = []
    name = tree.name
    name_obj = get_obj_from_code_str(name, '', globs, locs)
    if isinstance(name_obj, str):
        ret.append(f'${name_obj}')
    else:
        ret.append('lit')
    return ret


def analyze_call(tree, globs, locs):
    ret = []
    func_name = get_func_name(tree)
    if func_name == 'col':
        arg = tree.args[0]
        if isinstance(arg, astroid.nodes.Const):
            ret.append(f"${arg.as_string()}")
        else:
            col_name = get_obj_from_code_str(arg.as_string(), '', globs, locs)
            ret.append(f"${col_name}")
        return ret
    elif func_name == 'lit':
        ret.append('lit')
        return ret
    elif func_name == 'alias':
        return analyze_alias(tree, globs, locs)
    elif func_name in aggregate_functions:
        return analyze_udaf(tree, globs, locs)
    elif func_name in builtin_functions:
        return analyze_builtin_functions(tree, globs, locs)
    elif func_name in builtin_udf:
        return analyze_builtin_udf(tree, globs, locs)
    else:
        return analyze_udf(tree, globs, locs)


def analyze_alias(tree, globs, locs):
    return analyze_dependency(tree.func.expr, globs, locs)


def analyze_udaf(tree, globs, locs):
    arg = tree.args[0]
    return analyze_dependency(arg, globs, locs)


def analyze_builtin_functions(tree, globs, locs):
    ret = []
    ret.extend(analyze_dependency(tree.func.expr, globs, locs))
    func_name = get_func_name(tree)
    for i, arg in enumerate(tree.args):
        if i in builtin_functions[func_name]:
            ret.extend(analyze_dependency(arg, globs, locs))
    return ret


def analyze_builtin_udf(tree, globs, locs):
    ret = []
    func_name = get_func_name(tree)
    for i, arg in enumerate(tree.args):
        if i in builtin_udf[func_name]:
            ret.extend(analyze_dependency(arg, globs, locs))
    return ret


def analyze_udf(tree, globs, locs):
    ret = []
    for arg in tree.args:
            ret.extend(analyze_dependency(arg, globs, locs))
    return ret


def analyze_binop(tree, globs, locs):
    ret = []
    ret.extend(analyze_dependency(tree.left, globs, locs))
    ret.extend(analyze_dependency(tree.right, globs, locs))
    return ret


def analyze_comp(tree, globs, locs):
    ret = []
    ret.extend(analyze_dependency(tree.left, globs, locs))
    for op in tree.ops:
        ret.extend(analyze_dependency(op[1], globs, locs))
    return ret


def analyze_attr(tree, globs, locs):
    ret = []
    df = tree.expr.as_string()
    col = tree.attrname
    ret.append(f"{df}${col}")
    return ret

def analyze_subscript(tree, globs, locs):
    ret = []
    is_string_slice = check_slice(tree, globs, locs)
    if is_string_slice: # df["col"]
        df = tree.value.as_string()
        slice_value_node = tree.slice.value
        slice_value_code = slice_value_node.as_string()
        if isinstance(slice_value_node, astroid.nodes.Name):
            slice_value = get_obj_from_code_str(slice_value_code, '', globs, locs)
        if isinstance(slice_value_node, astroid.nodes.Const):
            slice_value = slice_value_node.value
        col = slice_value
        ret.append(f"{df}${col}")
    else: # col[1] 
        ret.extend(analyze_dependency(tree.value, globs, locs))
    return ret