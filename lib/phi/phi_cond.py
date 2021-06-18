import astroid
from pyspark.sql.types import *
from pyspark.sql.functions import *
from lib.phi.util import *
from lib.phi.constant import *
from lib.phi.phi_col import *

def phi_cond(code, df, globs, locs):
    tree = code2node(code)
    if isinstance(tree, astroid.nodes.Const):
        return phi_cond_col(code, df, globs, locs)
    if isinstance(tree, astroid.nodes.Call):
        func_name = get_func_name(tree)
        if isinstance(tree.func, astroid.nodes.Name) or func_name == 'col' or func_name == 'lit':
            return phi_cond_col(code, df, globs, locs)
        elif isinstance(tree.func, astroid.nodes.Attribute):
            return phi_cond_attr(code, df, globs, locs)
        else:
            return phi_cond_col(code, df, globs, locs)
            # raise ValueError(f'unsupported node type in condition: {tree.repr_tree()}')
    elif isinstance(tree, astroid.nodes.Name):
        return phi_cond_col(code, df, globs, locs)
    elif isinstance(tree, astroid.nodes.Compare):
        return phi_cond_comp(code, df, globs, locs)
    elif isinstance(tree, astroid.nodes.BoolOp):
        return phi_cond_boolop(code, df, globs, locs)
    elif isinstance(tree, astroid.nodes.BinOp):
        return phi_cond_binop(code, df, globs, locs)
    elif isinstance(tree, astroid.nodes.UnaryOp):
        return phi_cond_unaryop(code, df, globs, locs)
    else:
        raise ValueError(f'unsupported node type in condition: {tree.repr_tree()}')


def phi_cond_col(code, df, globs, locs):
    code_export_list = []
    new_col_code, export_code = phi_col(code, df, globs, locs)
    code_export_list.extend(export_code.split('\n'))
    col_type = get_value_type(new_col_code, export_code, df, globs, locs)
    code_export_list.extend(build_get_value_udf(col_type).split('\n'))
    col_value_code = get_value_code(new_col_code, col_type)
    return col_value_code, '\n'.join(code_export_list)



def phi_cond_attr(code, df, globs, locs):
    # e.g., col('a').endswith('xxx') -> get_value(phi(col('a'))).endswith('xxx')
    code_export_list = []
    tree = astroid.extract_node(code)
    assert isinstance(tree.func, astroid.nodes.Attribute)
    col_code = tree.func.expr.as_string()
    new_col_code, export_code = phi_col(col_code, df, globs, locs)
    code_export_list.extend(export_code.split('\n'))
    col_type = get_value_type(new_col_code, export_code, df, globs, locs)
    code_export_list.extend(build_get_value_udf(col_type).split('\n'))
    col_value_code = get_value_code(new_col_code, col_type)
    tree.func.expr = astroid.extract_node(col_value_code)
    return tree.as_string(), '\n'.join(code_export_list)


# actually pyspark does not support and, or, not, it uses |, &, ~ instead
def phi_cond_boolop(code, df, globs, locs):
    code_export_list = []
    tree = astroid.extract_node(code)
    new_values = []
    for value in tree.values:
        cond_code = value.as_string()
        new_cond_code, export_code = phi_cond(cond_code, df, globs, locs)
        code_export_list.extend(export_code.split('\n'))
        new_values.append(astroid.extract_node(new_cond_code))
    tree.values = new_values
    new_code = f'{tree.as_string()}.cast(BooleanType())' # need a cast operation here
    return new_code, '\n'.join(code_export_list)


def phi_cond_binop(code, df, globs, locs):
    code_export_list = []
    tree = astroid.extract_node(code)
    # left
    left_cond_code = tree.left.as_string()
    new_left_cond_code, export_code = phi_cond(left_cond_code, df, globs, locs)
    code_export_list.extend(export_code.split('\n'))
    tree.left = astroid.extract_node(new_left_cond_code)
    # right
    right_cond_code = tree.right.as_string()
    new_right_cond_code, export_code = phi_cond(right_cond_code, df, globs, locs)
    code_export_list.extend(export_code.split('\n'))
    tree.right = astroid.extract_node(new_right_cond_code)
    return tree.as_string(), '\n'.join(code_export_list)


def phi_cond_unaryop(code, df, globs, locs):
    code_export_list = []
    tree = astroid.extract_node(code)
    operand_cond_code = tree.operand.as_string()
    new_operand_cond_code, export_code = phi_cond(operand_cond_code, df, globs, locs)
    code_export_list.extend(export_code.split('\n'))
    tree.operand = astroid.extract_node(new_operand_cond_code)
    return tree.as_string(), '\n'.join(code_export_list)


def phi_cond_comp(code, df, globs, locs):
    code_export_list = []
    tree = astroid.extract_node(code)
    # left col
    left_col_code = tree.left.as_string()
    new_left_col_code, export_code = phi_col(left_col_code, df, globs, locs)
    code_export_list.extend(export_code.split('\n'))
    col_type = get_value_type(new_left_col_code, export_code, df, globs, locs)
    code_export_list.extend(build_get_value_udf(col_type).split('\n'))
    left_col_value_code = get_value_code(new_left_col_code, col_type)
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
        new_ops.append([op[0], astroid.extract_node(right_col_value_code)])

    # reconstruct tree
    tree.left = astroid.extract_node(left_col_value_code)
    tree.ops = new_ops
    return tree.as_string(), '\n'.join(code_export_list)

if __name__ == "__main__":
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
    def length(s):
        return len(str(s))
    
    # test comp
    new_code, export_code = phi_cond("length(col('FileName')) > 10", "documents", globals(), locals())
    exec(f"""{export_code}
documents.where({new_code}).select(col('FileName')).show()""")

    # test binop
    new_code, export_code = phi_cond("(length(col('FileName')) > 10) & (length(col('FileName')) < 20)", "documents", globals(), locals())
    exec(f"""{export_code}
documents.where({new_code}).select(col('FileName')).show()""")
    
    # test binop
    new_code, export_code = phi_cond("~(length(col('FileName')) > 10)", "documents", globals(), locals())
    exec(f"""{export_code}
documents.where({new_code}).select(col('FileName')).show()""")

    # test attr
    new_code, export_code = phi_cond("col('FileName').endswith('pdf')", "documents", globals(), locals())
    exec(f"""{export_code}
documents.where({new_code}).select(col('FileName')).show()""")