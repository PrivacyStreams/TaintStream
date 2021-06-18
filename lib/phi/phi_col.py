import astroid
from pyspark.sql.types import *
from pyspark.sql.functions import *
from lib.phi.util import *
from lib.phi.constant import *
from lib.phi.pudf import *

# generate according to the pyspark doc
aggregate_functions = [
    'avg', 'max', 'min', 'sum', 'count', 'approx_count_distinct', 'collect_list', 'collect_set',
    'first', 'grouping', 'grouping_id', 'kurtosis', 'last', 'mean', 'skewness', 'stddev', 'stddev_pop',
    'stddev_samp', 'sumDistinct', 'var_pop', 'var_samp', 'variance'
]

# functions like: col.endswith() ...
builtin_functions = {
    'endswith': [], # no argument is a col
    'isNotNull': [],
    'isNull': [],
    'rlike': [],
    'getItem': [],
    'cast': []
}

# functions like regexp_replace(col, ...) ...
builtin_udf = {
    'regexp_replace': [0], # No. 0 argument is a col
    'size': [0],
    'sqrt': [0],
    'pow': [0,1],
    'length': [0],
    'when': [0,1],
    'otherwise': [0],
    'concat': [0,1,2,3,4,5,6,7,8,9,10,11], # all the columns are col,
    'to_timestamp': [0],
    'explode': [0],
    'regexp_extract': [0],
    'trim': [0],
    'lower': [0],
    'split': [0],
    'base64': [0],
    'concat_ws': [1,2,3,4,5,6,7,8,9,10,11],
    'from_json': [0]
}

def phi_col(code, df, globs, locs):
    # astroid can not extract the const string node, so use ast here
    # try:
    #     tree = astroid.extract_node(code)
    # except ValueError as e:
    #     import ast
    #     tree = ast.parse(code)
    #     if len(tree.body) == 1 and isinstance(tree.body[0].value, ast.Str):
    #         return code, ''
    #     else:
    #         raise e
    tree = code2node(code)
    code_export_list = []
    # print(tree.repr_tree())
    if isinstance(tree, astroid.nodes.Const):
        if isinstance(tree.value, str): # string type has two case
            print("[WARN] Detect a direct str use as a column. By default, TaintStream consider str '' as lit and other strs as col.")
            print("It is recommended to use col()/lit() api to explitcitly define the usage purpose.")
            if code == "''" or code == '""':
                return phi_lit(f"lit({code})", df, globs, locs)
            else:
                return code, '\n'.join(code_export_list)
        else: # other types are considered as lit
            return phi_lit(f"lit({code})", df, globs, locs)
    elif isinstance(tree, astroid.nodes.Name):
        name = tree.name
        print("[WARN] Detect a variable. By default, TaintStream consider str as column name and other types as lit.")
        print("It is recommended to use col()/lit() api to explitcitly define the usage purpose.")
        name_obj = get_obj_from_code_str(name, '', globs, locs)
        if isinstance(name_obj, str):
            return code, '\n'.join(code_export_list)
        else:
            return phi_lit(f"lit({code})", df, globs, locs)
    elif isinstance(tree, astroid.nodes.Call):
        func_name = get_func_name(tree)
        if func_name == 'col':
            return code, '\n'.join(code_export_list)
        elif func_name == 'lit':
            return phi_lit(code, df, globs, locs)
        elif func_name == 'alias':
            return phi_alias(code, df, globs, locs)
        elif func_name in aggregate_functions:
            return phi_udaf(code, df, globs, locs)
        # F_base(c_1, c_2, ...)
        elif func_name in builtin_functions or func_name in builtin_udf:
            return phi_builtin(code, df, globs, locs)
        else:
            return phi_udf(code, df, globs, locs)
    elif isinstance(tree, astroid.nodes.BinOp):
        # c_1 op c_2
        return phi_binop(code, df, globs, locs)
    elif isinstance(tree, astroid.nodes.Compare):
        return phi_comp(code, df, globs, locs)
    # elif isinstance(tree, astroid.nodes.Const):
        # c
        # if isinstance(tree.value, str):
        #     return code, '\n'.join(code_export_list)
        # lit
        # else:
        #     return phi_lit(f"lit({code})", df, globs, locs)
    elif isinstance(tree, astroid.nodes.Attribute):
        # stmt.c
        return phi_attr(code, df, globs, locs)
    elif isinstance(tree, astroid.nodes.Subscript):
        return phi_subscript(code, df, globs, locs)
    else:
        raise ValueError(f"unsupported column: {tree.repr_tree()}")


def phi_alias(code, df, globs, locs):
    tree = astroid.extract_node(code)
    assert isinstance(tree.func, astroid.nodes.Attribute)
    prev_col = tree.func.expr.as_string()
    new_prev_col, export_code = phi_col(prev_col, df, globs, locs)
    new_expr = astroid.extract_node(new_prev_col)
    tree.func.expr = new_expr
    new_code = tree.as_string()
    new_code = remove_duplicated_alias(new_code)
    return new_code, export_code


def phi_binop(code, df, globs, locs):
    tree = astroid.extract_node(code)
    code_export_list = []
    col_types = []
    from lib.phi.constant import tag_type
    code_export_list.extend(build_get_tag_udf(tag_type).split('\n'))
    # left
    left_code = tree.left.as_string()
    new_left_code, left_export_code = phi_col(left_code, df, globs, locs)
    code_export_list.extend(left_export_code.split('\n'))
    left_type = get_value_type(new_left_code, left_export_code, df, globs, locs)
    if left_type not in col_types:
        # build for each type instead of for each col
        code_export_list.extend(build_get_value_udf(left_type).split('\n'))
        col_types.append(left_type)
    left_value_code = get_value_code(new_left_code, left_type)
    left_tag_code = get_tag_code(new_left_code)
    # right
    right_code = tree.right.as_string()
    new_right_code, right_export_code = phi_col(right_code, df, globs, locs)
    code_export_list.extend(right_export_code.split('\n'))
    right_type = get_value_type(new_right_code, right_export_code, df, globs, locs)
    if right_type not in col_types:
        # build for each type instead of for each col
        code_export_list.extend(build_get_value_udf(right_type).split('\n'))
        col_types.append(right_type)
    right_value_code = get_value_code(new_right_code, right_type)
    right_tag_code = get_tag_code(new_right_code)
    # pack
    try:
        tree.left = astroid.extract_node(left_value_code)
        tree.right = astroid.extract_node(right_value_code)
        ret_value_code = tree.as_string()
    except ValueError:
        ret_value_code = f"({tree.op.join([left_value_code, right_value_code])})"
    ret_type = get_col_type(ret_value_code, '\n'.join(code_export_list), df, globs, locs)
    code_export_list.extend(build_pack_udf(ret_type).split('\n'))
    from lib.phi.constant import tag_type
    code_export_list.extend(build_col_agg_udf(tag_type).split('\n'))
    # new_code = f"pack_{ret_type}({ret_value_code}, col_agg({left_tag_code}, {right_tag_code}))"
    new_code = pack_code(ret_type, ret_value_code, col_agg_code(left_tag_code, right_tag_code))
    # restore original column name
    new_code = restore_col_name(code, new_code, globs, locs)
    new_code = remove_duplicated_alias(new_code)
    return new_code, '\n'.join(code_export_list)

# ==, >, < ...
def phi_comp(code, df, globs, locs):
    code_export_list = []
    tree = astroid.extract_node(code)
    from lib.phi.constant import tag_type
    code_export_list.extend(build_get_tag_udf(tag_type).split('\n'))
    tags_code_list = []
    # left col
    left_col_code = tree.left.as_string()
    new_left_col_code, export_code = phi_col(left_col_code, df, globs, locs)
    code_export_list.extend(export_code.split('\n'))
    col_type = get_value_type(new_left_col_code, export_code, df, globs, locs)
    code_export_list.extend(build_get_value_udf(col_type).split('\n'))
    left_col_value_code = get_value_code(new_left_col_code, col_type)
    left_col_tag_code = get_tag_code(new_left_col_code)
    tags_code_list.append(left_col_tag_code)
    # right col
    ops = tree.ops
    new_ops = []
    for op in ops:
        right_col_code = op[1].as_string()
        new_right_col_code, export_code = phi_col(right_col_code, df, globs, locs)
        code_export_list.extend(export_code.split('\n'))
        col_type = get_value_type(new_right_col_code, export_code, df, globs, locs)
        code_export_list.extend(build_get_value_udf(col_type).split('\n'))
        right_col_value_code = get_value_code(new_right_col_code, col_type)
        right_col_tag_code = get_tag_code(new_right_col_code)
        tags_code_list.append(right_col_tag_code)
        new_ops.append([op[0], astroid.extract_node(right_col_value_code)])
    # reconstruct tree
    tree.left = astroid.extract_node(left_col_value_code)
    tree.ops = new_ops
    value_code = tree.as_string()
    # pack
    ret_type = get_col_type(value_code, '\n'.join(code_export_list), df, globs, locs)
    code_export_list.extend(build_pack_udf(ret_type).split('\n'))
    from lib.phi.constant import tag_type
    code_export_list.extend(build_col_agg_udf(tag_type).split('\n'))
    # new_code = f"pack_{ret_type}({value_code}, col_agg({', '.join(tags_code_list)}))"
    new_code = pack_code(ret_type, value_code, col_agg_code(*tags_code_list))
    new_code = restore_col_name(code, new_code, globs, locs)
    new_code = remove_duplicated_alias(new_code)
    return new_code, '\n'.join(code_export_list)
    

    
def phi_lit(code, df, globs, locs):
    tree = astroid.extract_node(code)
    if isinstance(tree, astroid.nodes.Const):
        code = f"lit({code})"
        tree =  astroid.extract_node(code)
    code_export_list = []
    code_export_list.append('from pyspark.sql.functions import lit')
    
    col_type = get_col_type(code, '\n'.join(code_export_list), df, globs, locs)
    code_export_list.extend(build_pack_udf(col_type).split('\n'))
    code_export_list.extend(build_lit_tag_udf(tag_type).split('\n'))
    # new_code = f"pack_{col_type}({code}, lit_tag())"
    new_code = pack_code(col_type, code, "lit_tag()")
    # restore original column name
    new_code = restore_col_name(code, new_code, globs, locs)
    new_code = remove_duplicated_alias(new_code)
    return new_code, '\n'.join(code_export_list)


# phi(udf(col_1, col_2, ...)) => pack(udf(val(col_1), val(col_2), ...), col_agg(tag(col_1), tag(col_2), ...))
def phi_udf(code, df, globs, locs):
    code_export_list = [] # code needs to be defined outside udf, i.e., defined functions of pack(), get_tag() ... 
    tree = astroid.extract_node(code)
    # print(tree.repr_tree())
    assert isinstance(tree, astroid.nodes.Call)
    udf_name = tree.func.as_string()
    
    # build pack function
    # udf_obj = get_obj_from_code_str(udf_name, '', globs, locs)
    # udf_ret_type = udf_obj.returnType
    # code_export_list.extend(build_pack_udf(udf_ret_type).split('\n'))
    
    # build get_value function
    col_types = []
    col_vs = []
    col_ts = []
    for arg in tree.args:
        col_code = arg.as_string()
        new_col_code, export_code = phi_col(col_code, df, globs, locs)
        code_export_list.extend(export_code.split('\n'))
        col_type = get_value_type(new_col_code, export_code, df, globs, locs)
        if col_type not in col_types:
            # build for each type instead of for each arguments col
            code_export_list.extend(build_get_value_udf(col_type).split('\n'))
            col_types.append(col_type)
        # build col_vs and col_ts
        col_vs.append(get_value_code(new_col_code, col_type)) # actually, here should be phi_col(col_code)
        col_ts.append(get_tag_code(new_col_code))
        
    # build get_tag function
    from lib.phi.constant import tag_type
    code_export_list.extend(build_get_tag_udf(tag_type).split('\n'))
    
    # build col_agg function
    code_export_list.extend(build_col_agg_udf(tag_type).split('\n'))

    # build pack
    udf_ret_type = get_col_type(f"{tree.func.as_string()}({', '.join(col_vs)})", '\n'.join(code_export_list), df, globs, locs)
    code_export_list.extend(build_pack_udf(udf_ret_type).split('\n'))
    
    # perform transform on code
    # new_code = f"pack_{udf_ret_type}({udf_name}({', '.join(col_vs)}), col_agg({', '.join(col_ts)}))"
    if len(col_vs) == 0 and len(col_ts) == 0:
        code_export_list.extend(build_lit_tag_udf(BooleanType()).split('\n'))
    new_code = pack_code(udf_ret_type, f"{tree.func.as_string()}({', '.join(col_vs)})", col_agg_code(*col_ts))
    # restore original column name
    new_code = restore_col_name(code, new_code, globs, locs)
    new_code = remove_duplicated_alias(new_code)
    return new_code, '\n'.join(code_export_list)


def phi_attr(code, df, globs, locs):
    # from lib.phi.phi_stmt import phi_stmt
    # tree = astroid.extract_node(code)
    # code_export_list = []
    # stmt = tree.expr.as_string()
    # new_stmt, export_code = phi_stmt(stmt, stmt, globs, locs)
    # code_export_list.extend(export_code.split('\n'))
    # tree.expr = astroid.extract_node(new_stmt)
    # new_code = tree.as_string()
    # restore original column name
    # new_code = restore_col_name(code, new_code, globs, locs)
    return code, ''

# check slice type is string (True) or not (False)
def check_slice(node, globs, locs):
    try:
        slice_value_node = node.slice.value
        slice_value_code = slice_value_node.as_string()
        if isinstance(slice_value_node, astroid.nodes.Name):
            slice_value = get_obj_from_code_str(slice_value_code, '', globs, locs)
        if isinstance(slice_value_node, astroid.nodes.Const):
            slice_value = slice_value_node.value
        return isinstance(slice_value, str)
    except:
        return False
        


def phi_subscript(code, df, globs, locs):
    from lib.phi.phi_stmt import phi_stmt
    tree = astroid.extract_node(code)
    code_export_list = []
    is_string_slice = check_slice(tree, globs, locs)
    if is_string_slice:
        stmt = tree.value.as_string()
        new_stmt, export_code = phi_stmt(stmt, stmt, globs, locs)
        code_export_list.extend(export_code.split('\n'))
        tree.value = astroid.extract_node(new_stmt)
        new_code = tree.as_string()
    else:
        col_code = tree.value.as_string()
        new_col_code, export_code = phi_col(col_code, df, globs, locs)
        code_export_list.extend(export_code.split('\n'))
        col_type = get_value_type(new_col_code, export_code, df, globs, locs)
        # value
        code_export_list.extend(build_get_value_udf(col_type).split('\n'))
        tree.value = astroid.extract_node(get_value_code(new_col_code, col_type))
        value_code = tree.as_string() 
        # code
        from lib.phi.constant import tag_type
        code_export_list.extend(build_get_tag_udf(tag_type).split('\n'))
        tag_code = get_tag_code(new_col_code)
        # pack
        new_code = pack_code(col_type, value_code, tag_code)
        new_code = restore_col_name(code, new_code, globs, locs)
        new_code = remove_duplicated_alias(new_code)
    return new_code, '\n'.join(code_export_list)
        

    # restore original column name
    # new_code = restore_col_name(code, new_code, globs, locs)
    


def phi_builtin(code, df, globs, locs):

    tree = astroid.extract_node(code)

    # print(tree.repr_tree())
    assert isinstance(tree, astroid.nodes.Call)
    func_name = get_func_name(tree)
    if func_name in builtin_udf:
        return phi_builtin_udf(code, df, globs, locs)
    elif func_name in builtin_functions:
        return phi_builtin_func(code, df, globs, locs)
    else:
        raise RewriteError(f"unsupported builtin: {code}, type: {tree.repr_tree()}")


# e.g., col('name').endwith('xxx')
def phi_builtin_func(code, df, globs, locs):
    code_export_list = []
    tree = astroid.extract_node(code)
    func_name = get_func_name(tree)
    
    # col in expr
    col_code = tree.func.expr.as_string()
    new_col_code, export_code = phi_col(col_code, df, globs, locs)
    code_export_list.extend(export_code.split('\n'))
    col_type = get_value_type(new_col_code, export_code, df, globs, locs)
    code_export_list.extend(build_get_value_udf(col_type).split('\n'))
    from lib.phi.constant import tag_type
    code_export_list.extend(build_get_tag_udf(tag_type).split('\n'))
    col_value_code = get_value_code(new_col_code, col_type)
    col_tag_code = get_tag_code(new_col_code)
    tree.func.expr = astroid.extract_node(col_value_code)
    
    # return value of the built in func, note that some 
    # arguments in builtin function (almost all) can be basic types like str.
    # This infomation is saved in builtin_functions.
    col_types = []
    col_vs = []
    col_ts = []
    col_ts.append(col_tag_code) # note that the tag of col before '.' should also be aggregated
    for i, arg in enumerate(tree.args):
        if i in builtin_functions[func_name]:
            col_code = arg.as_string()
            new_col_code, export_code = phi_col(col_code, df, globs, locs)
            code_export_list.extend(export_code.split('\n'))
            col_type = get_value_type(new_col_code, export_code, df, globs, locs)
            if col_type not in col_types:
                # build for each type instead of for each arguments col
                code_export_list.extend(build_get_value_udf(col_type).split('\n'))
                col_types.append(col_type)
            # build col_vs and col_ts
            col_vs.append(get_value_code(new_col_code, col_type)) # actually, here should be phi_col(col_code)
            col_ts.append(get_tag_code(new_col_code))
        else:
            col_vs.append(arg.as_string())

    # build get_tag function
    from lib.phi.constant import tag_type
    code_export_list.extend(build_get_tag_udf(tag_type).split('\n'))
    
    # build col_agg function
    code_export_list.extend(build_col_agg_udf(tag_type).split('\n'))

    # build pack
    udf_ret_type = get_col_type(f"{tree.func.as_string()}({', '.join(col_vs)})", '\n'.join(code_export_list), df, globs, locs)
    code_export_list.extend(build_pack_udf(udf_ret_type).split('\n'))
    
    # perform transform on code
    # new_code = f"pack_{udf_ret_type}({tree.func.as_string()}({', '.join(col_vs)}), col_agg({', '.join(col_ts)}))"
    new_code = pack_code(udf_ret_type, f"{tree.func.as_string()}({', '.join(col_vs)})", col_agg_code(*col_ts))
    # restore original column name
    new_code = restore_col_name(code, new_code, globs, locs)
    new_code = remove_duplicated_alias(new_code)
    return new_code, '\n'.join(code_export_list)


# e.g., regexp_replace
def phi_builtin_udf(code, df, globs, locs):
    code_export_list = [] # code needs to be defined outside udf, i.e., defined functions of pack(), get_tag() ... 
    tree = astroid.extract_node(code)
    udf_name = get_func_name(tree)

    # return value of the builtin udf, note that some arguments in builtin udf can be basic type like str, 
    # this infomation is saved in builtin_functions.
    col_types = []
    col_vs = []
    col_ts = []
    
    if isinstance(tree.func, astroid.nodes.Attribute) and isinstance(tree.func.expr, astroid.nodes.Call):
        # we find several builtin udf may be called sequently, e.g., when(cond, value).otherwise(col)
        new_code, export_code = phi_col(tree.func.expr.as_string(), df, globs, locs)
        code_export_list.extend(export_code.split('\n'))
        col_type = get_value_type(new_code, export_code, df, globs, locs)
        code_export_list.extend(build_get_value_udf(col_type).split('\n'))
        # previous value
        new_value_code = get_value_code(new_code, col_type)
        tree.func.expr = astroid.extract_node(new_value_code)
        # previous tag
        col_ts.append(get_tag_code(new_code))
    
    
    for i, arg in enumerate(tree.args):
        if i in builtin_udf[udf_name]:
            col_code = arg.as_string()
            new_col_code, export_code = phi_col(col_code, df, globs, locs)
            code_export_list.extend(export_code.split('\n'))
            col_type = get_value_type(new_col_code, export_code, df, globs, locs)
            if col_type not in col_types:
                # build for each type instead of for each arguments col
                code_export_list.extend(build_get_value_udf(col_type).split('\n'))
                col_types.append(col_type)
            # build col_vs and col_ts
            col_vs.append(get_value_code(new_col_code, col_type))
            col_ts.append(get_tag_code(new_col_code))
        else:
            col_vs.append(arg.as_string())

            
    # build get_tag function
    from lib.phi.constant import tag_type
    code_export_list.extend(build_get_tag_udf(tag_type).split('\n'))
    
    # build col_agg function
    code_export_list.extend(build_col_agg_udf(tag_type).split('\n'))

    # build pack
    udf_ret_type = get_col_type(f"{tree.func.as_string()}({', '.join(col_vs)})", '\n'.join(code_export_list), df, globs, locs)
    code_export_list.extend(build_pack_udf(udf_ret_type).split('\n'))
    
    # perform transform on code
    # new_code = f"pack_{udf_ret_type}({udf_name}({', '.join(col_vs)}), col_agg({', '.join(col_ts)}))"
    new_code = pack_code(udf_ret_type, f"{tree.func.as_string()}({', '.join(col_vs)})", col_agg_code(*col_ts))
    # restore original column name
    new_code = restore_col_name(code, new_code, globs, locs)
    new_code = remove_duplicated_alias(new_code)
    return new_code, '\n'.join(code_export_list)


def phi_udaf(code, df, globs, locs):
    code_export_list = [] # code needs to be defined outside udf, i.e., defined functions of pack(), get_tag() ... 
    tree = astroid.extract_node(code)
    assert isinstance(tree, astroid.nodes.Call)
    assert len(tree.args) == 1
    udaf_name = get_func_name(tree)
    arg_code = tree.args[0].as_string()
    # specially process for count("*"), any other udaf support "*"?
    if udaf_name == 'count' and '*' in arg_code:
        col_value_type = LongType() # count() always return long type
        df_schema = get_schema(df, globs, locs)
        from lib.phi.constant import tag_type
        code_export_list.extend(build_get_tag_udf(tag_type).split('\n'))
        code_export_list.extend(build_row_agg_pudf(tag_type).split('\n'))
        code_export_list.extend(build_lit_tag_udf(tag_type).split('\n'))
        # code_export_list.extend(build_col_agg_udf(tag_type).split('\n'))

        # flatten the * to columns
        # row_agg_args = []
        # for field in df_schema:
        #     row_agg_args.append(get_tag_code('"' + field.name + '"'))
        row_agg_code = f"row_agg(lit_tag())" # for count(*), consider its tag is False

        code_export_list.extend(build_pack_udf(col_value_type).split('\n'))
        new_code = pack_code(col_value_type, tree.as_string(), row_agg_code)
        new_code = restore_col_name(code, new_code, globs, locs)
        new_code = remove_duplicated_alias(new_code)
        return new_code, '\n'.join(code_export_list)
    else:
        new_arg_code, export_code = phi_col(arg_code, df, globs, locs)
        col_value_type = get_value_type(new_arg_code, export_code, df, globs, locs)
        tree.args[0] = code2node(get_value_code(new_arg_code, col_value_type))
        code_export_list.extend(export_code.split('\n'))

        
        udaf_return_type = name2returnType[udaf_name]
        if udaf_return_type == -1:
            if udaf_name in return_array_udaf:
                udaf_return_type = ArrayType(col_value_type)
            elif udaf_name in type_extented_udaf:
                udaf_return_type = extend_precision(col_value_type)
            else:
                udaf_return_type = col_value_type
        code_export_list.extend(build_pack_udf(udaf_return_type).split('\n'))
        code_export_list.extend(build_get_value_udf(col_value_type).split('\n'))
        from lib.phi.constant import tag_type
        # code_export_list.extend(name2func[udaf_name](udaf_return_type).split('\n')) # udaf_name 2 pandas udf used to over write builtin aggregate function
        code_export_list.extend(build_get_tag_udf(tag_type).split('\n'))
        code_export_list.extend(build_row_agg_pudf(tag_type).split('\n'))


        # new_code = pack_code(udaf_return_type, f"pudf_{udaf_name}_{format_type(udaf_return_type)}({get_value_code(new_arg_code, col_value_type)})", f"row_agg({get_tag_code(new_arg_code)})")
        new_code = pack_code(udaf_return_type, tree.as_string(), f"row_agg({get_tag_code(new_arg_code)})")
        new_code = restore_col_name(code, new_code, globs, locs)
        new_code = remove_duplicated_alias(new_code)
        return new_code, '\n'.join(code_export_list)
    
    
def extend_precision(data_type):
    if isinstance(data_type, (IntegerType, ShortType, ByteType)):
        return LongType()
    elif isinstance(data_type, FloatType):
        return DoubleType()
    else:
        return data_type


def test():
    import sys
    import os
    import traceback
    import pyspark
    from pyspark.sql import SparkSession, DataFrame

    # libs for taint tarcking
    def taint_col(df, col):
        # df.printSchema()
        # return df
        for column in df.columns:
            datatType = df.schema[column].dataType
            if isinstance(datatType, TimestampType):
                # pyspark has problem when converting timestamps between builtin spark type and python type, just cast them to string
                # see details in https://stackoverflow.com/questions/50885719/pyspark-cant-do-column-operations-with-datetime-years-0001
                df = df.withColumn(column, df[column].cast("string"))
                datatType = StringType()
            packed_schema = StructType([
                StructField("value", datatType, True),
                StructField("tag", BooleanType(), False)
            ])
            @udf(packed_schema)
            def pack(col_v, col_t):
                return (col_v, col_t)
            if column == col:
                df = df.withColumn(column, pack(df[column], lit(True)))
            else:
                df = df.withColumn(column, pack(df[column], lit(False)))
        df.printSchema()
        # df.show()
        return df

    groundTruthDataFile = "/home/yuancli/chengxu/chengxu/test_scripts/data/documents_input"
    file_path = "/home/yuancli/chengxu/chengxu/test_scripts/data/documents_input"
    outputFile = f"{__file__[:-3]}._output.csv"

    appName = "symbolic_test"
    spark = SparkSession.builder.appName(appName).getOrCreate()

    groundTruth = spark.read.json(groundTruthDataFile)

    # documents = spark.read.json(file_path) -->
    documents = taint_col(spark.read.json(file_path), "Author")

    @udf(IntegerType())
    def length(s, ss):
        return len(str(s)) + len(str(ss))
    
    # test compare
    new_code, export_code = phi_col("length(col('FileName'), col('Author')) > 10", "documents", globals(), locals())

    exec(f"""{export_code}
documents.select({new_code}).show()""")
    
    # test udf
    new_code, export_code = phi_col("length(col('FileName'), col('Author'))", "documents", globals(), locals())

    exec(f"""{export_code}
documents.select({new_code}).show()""")

    @udf(LongType())
    def sum_udf(a, b):
        return a + b

    # test binop
    new_code, export_code = phi_col("sum_udf(col('DocId') - length(col('DocId'),col('Author')), col('DocId')).alias('sum')", "documents", globals(), locals())
    
    exec(f"""{export_code}
documents.select({new_code}).show()""")

    # test lit
    new_code, export_code = phi_col("lit(-100) + col('DocId')", "documents", globals(), locals())
    exec(f"""{export_code}
documents.select({new_code}).show()""")

    # test built-in
    new_code, export_code = phi_col("col('FileName').endswith('pdf')", "documents", globals(), locals())

    exec(f"""{export_code}
documents.select({new_code}).show()""")