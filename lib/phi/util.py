from pyspark.sql.types import *
import traceback
# from pyspark.sql.functions import *
import astroid
from lib.phi.constant import *

def exception_handler(func):
    def wrapper(*args, **kw):
        try:
            return func(*args, **kw)
        except Exception as e:
            traceback.print_exc()
            raise TaintStreamError()
    return wrapper

@exception_handler
def code2node(code):
    try:
        return astroid.extract_node(code)
    except ValueError:
        # here is a bug of astroid
        tree = astroid.parse(code)
        return astroid.nodes.Const(tree.doc)

@exception_handler
def get_obj_from_code_str(code_str, export_code, globs, locs):
    try:
        exec(export_code, globs, locs)
        obj = eval(code_str, globs, locs)
        return obj
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise e

@exception_handler
def get_alias_name(code, globs, locs):
    tree = astroid.extract_node(code)
    alias_arg = tree.args[0] # arg[0] is the alias, the other are metadata
    return get_obj_from_code_str(alias_arg.as_string(), '', globs, locs)


# use a select clause to precisely get the schema of a column to further get its datatype
@exception_handler
def get_real_df_schema(code, export_code, df, globs, locs):
    try:
        exec(export_code, globs, locs)
        real_df, export_code = get_real_df(code, export_code, df, globs, locs)
        exec(export_code, globs, locs)
        schema_obj = eval(f"{real_df}.select({code}).schema", globs, locs)
    except:
        schema_obj = eval(f"{df}.select({code}).schema", globs, locs)
    return schema_obj

@exception_handler
def get_col_name(code, export_code, globs, locs):
    import time
    # st = time.time()
    try:
        exec(export_code, globs, locs)
        col_obj = get_obj_from_code_str(code, export_code, globs, locs)
        # Question: any other approach to extract the name of the column?
        if isinstance(col_obj, str):
            return col_obj
        tree = astroid.extract_node(code)
        if isinstance(tree, astroid.nodes.Call) and isinstance(tree.func, astroid.nodes.Attribute) and tree.func.attrname == 'alias':
            return get_alias_name(code, globs, locs)
        col_name_list = str(col_obj).split("'")[1:-1]
        col_name = "'".join(col_name_list)
        # et = time.time()
        # print("get_col_name cost:", et - st)
        return col_name
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise e

# df can be other df if df is an attribute node or subscript node
# e.g. df1.join(df2, df1.col_1 == *df2.col_2*)
@exception_handler
def get_real_df(code, export_code, df, globs, locs):
    exec(export_code, globs, locs)
    try:
        df_tree = astroid.extract_node(code)
    except ValueError as e:
        import ast
        tree = ast.parse(code)
        if len(tree.body) == 1 and isinstance(tree.body[0].value, ast.Str):
            return df, ''
        else:
            raise e
    code_export_list = []
    if isinstance(df_tree, astroid.nodes.Subscript):
        while isinstance(df_tree, astroid.nodes.Subscript):
            prev_df = df_tree.value.as_string()
            df_tree = df_tree.value
    elif isinstance(df_tree, astroid.nodes.Attribute):
        while isinstance(df_tree, astroid.nodes.Attribute):
            prev_df = df_tree.expr.as_string()
            df_tree = df_tree.expr
    elif isinstance(df_tree, astroid.nodes.Call) and isinstance(df_tree.func, astroid.nodes.Attribute):
        # remove func like alias...
        return get_real_df(df_tree.func.expr.as_string(), export_code, df, globs, locs)
    else:
        return df, ''
    from lib.phi.phi_stmt import phi_stmt
    new_prev_df, export_code = phi_stmt(prev_df, prev_df, globs, locs)
    code_export_list.extend(export_code.split('\n'))
    return new_prev_df, '\n'.join(code_export_list)

@exception_handler
def get_schema(df, globs, locs):
    schema = eval(f"{df}.schema", globs, locs)
    return schema

@exception_handler
def get_value_type(code, export_code, df, globs, locs):
    # exec(export_code, globs, locs)
    # col_name = get_col_name(code, export_code, globs, locs)
    # real_df, export_code = get_real_df(code, export_code, df, globs, locs)
    # exec(export_code, globs, locs)
    schema_obj = get_real_df_schema(code, export_code, df, globs, locs)
    col_name = schema_obj.fieldNames()[0]
    field_names = schema_obj[col_name].dataType.fieldNames()
    if "value" in field_names and "tag" in field_names: # basic column
        value_type = schema_obj[col_name].dataType['value'].dataType
    else: # struct column
        value_type = StructType()
        for field_name in field_names:
            value_type.add(
                field_name,
                data_type=schema_obj[col_name].dataType[field_name].dataType['value'].dataType,
                nullable=schema_obj[col_name].dataType[field_name].dataType['value'].nullable,
                metadata=schema_obj[col_name].dataType[field_name].dataType['value'].metadata
            )
    return value_type

@exception_handler
def get_col_type(code, export_code, df, globs, locs):
    # exec(export_code, globs, locs)
    # col_name = get_col_name(code, export_code, globs, locs)
    # real_df, export_code = get_real_df(code, export_code, df, globs, locs)
    # exec(export_code, globs, locs)
    # value_type = eval(f"{real_df}.select({code}).schema['{col_name}'].dataType", globs, locs)
    schema_obj = get_real_df_schema(code, export_code, df, globs, locs)
    col_name = schema_obj.fieldNames()[0]
    value_type = schema_obj[col_name].dataType
    return value_type

@exception_handler
def get_func_name(tree):
    if isinstance(tree.func, astroid.nodes.Name):
        return tree.func.name
    elif isinstance(tree.func, astroid.nodes.Attribute):
        return tree.func.attrname
    else:
        raise ValueError(f"new types of func node: {tree.func}")

@exception_handler
def format_type(col_type):
    if isinstance(col_type, ArrayType):
        return "ArrayType_"+ str(abs(hash(col_type)))
    if isinstance(col_type, StructType):
        return "StructType_"+ str(abs(hash(col_type)))
    if isinstance(col_type, DecimalType): # ignore precision
        return "DecimalType"
    else:
        return str(col_type)


# convert the type obj to the code that is used to define theis type whenn running.
# e.g., col_type = StringType, return "StringType()"
# e.g., col_type = ArrayType(StringType,true), return "ArrayType(StringType(), True)"
@exception_handler
def type_obj_to_code(col_type):
    if isinstance(col_type, ArrayType):
        return f"ArrayType({type_obj_to_code(col_type.elementType)}, {col_type.containsNull})"
    if isinstance(col_type, StructType):
        field_args = []
        for field in col_type:
            field_args.append(f"StructField('{field.name}', {type_obj_to_code(field.dataType)}, {field.nullable})")
        return f"StructType([{', '.join(field_args)}])"
    else:
        return f"{col_type}()"

@exception_handler
def build_pack_udf(return_type):
    if isinstance(return_type, (StructType, ArrayType, MapType)):
        # print(f"[INFO] detect a pack udf use with a return type of {return_type}")
        format_return_type = format_type(return_type)
        return f"""
packed_schema_{format_return_type} = StructType([
    StructField('value', {type_obj_to_code(return_type)}, True),
    StructField('tag', BooleanType(), False)
])

@udf(packed_schema_{format_return_type})
def pack_{format_return_type}(col_v, col_t):
    return (col_v, col_t)
"""
    else: # for basic type, we use scala udf, see details in lib/udf dir
        return ""

@exception_handler
def build_get_value_udf(col_type):
    if isinstance(col_type, (StructType, ArrayType, MapType)):
        print(f"[INFO] detect a get_value udf use with a return type of {col_type}")
        format_col_type = format_type(col_type)
        return f"""
@udf({type_obj_to_code(col_type)})
def get_value_{format_col_type}(row):
    from pyspark.sql import Row
    if isinstance(row, Row):
        if hasattr(row, "value") and hasattr(row, "tag") and len(row) == 2:
            return row["value"]
        else:
            ret = []
            for sub_row in row:
                ret.append(sub_row["value"])
            return tuple(ret)
    else:
        return row[0]
"""
    else: # for basic type, we use scala udf,  see details in lib/udf dir
        return ""

@exception_handler
def build_get_tag_udf(tag_type):
    if isinstance(tag_type, BooleanType):
        return "" # for all the get tag udf, we use scala udf,  see details in lib/udf dir
    else:
        raise NotImplementedError(f"TaintStream does not support tag type of {tag_type}")                 

@exception_handler
def build_lit_tag_udf(tag_type):
    from lib.phi.constant import type2default
    return f"""
def lit_tag():
    return lit({type2default[tag_type]})
"""

@exception_handler
def build_col_agg_udf(tag_type):
    if isinstance(tag_type, BooleanType):
        return "" # for all the get tag udf, we use scala udf,  see details in lib/udf dir
    else: 
        raise NotImplementedError(f"TaintStream do not support tag type of {tag_type}")

@exception_handler
def merge_export_code(*export_codes):
    # TODO - remove duplicated code
    return '\n'.join(export_codes)

@exception_handler
def get_value_code(col_code, col_type):
    try:
        tree = astroid.extract_node(col_code)
        # we can optimize to simplify the rewritten code
        if isinstance(tree, astroid.nodes.Call):
            while isinstance(tree.func, astroid.nodes.Attribute) and get_func_name(tree) == "alias":
                tree = tree.func.expr
                func_name = get_func_name(tree)
                if func_name[:4] == "pack":
                    return tree.args[0].as_string()
        return f"get_value_{format_type(col_type)}({col_code})"
    except:
        return f"get_value_{format_type(col_type)}({col_code})"

@exception_handler
def get_tag_code(col_code): 
    try:
        tree = astroid.extract_node(col_code)
        # we can optimize to simplify the rewritten code
        if isinstance(tree, astroid.nodes.Call):
            while isinstance(tree.func, astroid.nodes.Attribute) and get_func_name(tree) == "alias":
                tree = tree.func.expr
                func_name = get_func_name(tree)
                if func_name[:4] == "pack":
                    return tree.args[1].as_string()
        return f"get_tag_BooleanType({col_code})"
    except:
        return f"get_tag_BooleanType({col_code})"

# here we use value type instead of the packed type for simplicity
@exception_handler
def pack_code(value_type, value_code, tag_code):
    return f"pack_{format_type(value_type)}({value_code}, {tag_code})"

@exception_handler
def restore_col_name(code, phi_code, globs, locs):
    old_col_name = get_col_name(code, '', globs, locs)
    ret_node = astroid.nodes.Call()
    func_node = astroid.nodes.Attribute(attrname='alias')
    func_node.postinit(expr = astroid.extract_node(phi_code))
    ret_node.postinit(
        func=func_node,
        args=[astroid.nodes.Const(old_col_name)]
    )
    return ret_node.as_string()

def has_duplicated_alias(tree):
    try:
        return tree.func.attrname == "alias" and tree.func.expr.func.attrname == "alias"
    except:
        return False

@exception_handler
def remove_duplicated_alias(code):
    tree = code2node(code)
    while has_duplicated_alias(tree):
        tree.func.expr = tree.func.expr.func.expr
        code = tree.as_string()
    return code


@exception_handler
def col_agg_code(*cols):
    if len(cols) == 0:
        return "lit_tag()"
    if len(cols) == 1:
        return cols[0]
    else:
        new_cols = set()
        for col in cols:
            col_node = code2node(col)
            if isinstance(col_node, astroid.nodes.Call) and col_node.func.as_string() == "col_agg":
                new_cols.update([arg.as_string() for arg in col_node.args])
            else:
                new_cols.add(col)
        new_cols = list(new_cols)
        return f"col_agg({', '.join(new_cols)})"


if __name__ == "__main__":
    # test get col type
    import sys
    import os
    import traceback
    import pyspark
    from pyspark.sql import SparkSession, DataFrame

    groundTruthDataFile = "/home/yuancli/chengxu/chengxu/test_scripts/data/documents_input"
    file_path = "/home/yuancli/chengxu/chengxu/test_scripts/data/documents_input"
    outputFile = f"{__file__[:-3]}._output.csv"

    appName = "symbolic_test"
    spark = SparkSession.builder.appName(appName).getOrCreate()

    groundTruth = spark.read.json(groundTruthDataFile)

    # documents = spark.read.json(file_path) -->
    documents = spark.read.json(file_path)

    print(get_col_type("""when(col("Author") == "Ss", 1).otherwise(0)""", "", "documents", globals(), locals()))
