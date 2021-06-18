import astroid
from pyspark.sql.types import *
from pyspark.sql.functions import *
from lib.phi.util import *
from lib.phi.constant import *
from lib.phi.phi_col import *
from lib.phi.phi_cond import *
from lib.phi.pudf import *
from lib.phi.implicit import *


def phi_stmt(code, df, globs, locs):
    try:
        tree = astroid.extract_node(code)
    except ValueError as e:
        import ast
        tree = ast.parse(code)
        if len(tree.body) == 1 and isinstance(tree.body[0].value, ast.Str):
            return code, ''
        else:
            raise e
    code_export_list = []
    # df
    if isinstance(tree, astroid.nodes.Name):
        return code, ""
    # structured API
    if isinstance(tree, astroid.nodes.Call):
        func = tree.func
        assert isinstance(func, astroid.nodes.Attribute) # any other possible case?
        if func.attrname == 'agg':
            if isinstance(func.expr, astroid.nodes.Call) \
                and isinstance(func.expr.func, astroid.nodes.Attribute) \
                    and func.expr.func.attrname == "groupBy":
                # process groupBy and agg together
                return phi_groupby_agg(code, df, globs, locs)
            else:
                # single agg api with no groupby
                return phi_agg(code, df, globs, locs)
        # process previous df
        prev_df = func.expr.as_string()
        structured_api = func.attrname
        new_prev_df, export_code = phi_stmt(prev_df, df, globs, locs)
        code_export_list.extend(export_code.split('\n'))
        # update locs
        exec(export_code, globs, locs)
        # locs.update(locals())
        # phi select
        if structured_api == 'select' or structured_api == 'drop':
            new_arg_list = []
            for arg in tree.args:
                arg_str = arg.as_string()
                new_arg_str, export_code = phi_col(arg_str, new_prev_df, globs, locs)
                new_arg_list.append(new_arg_str)
                code_export_list.extend(export_code.split('\n'))
            return f"{new_prev_df}.{structured_api}({', '.join(new_arg_list)})", '\n'.join(code_export_list)
        elif structured_api == 'withColumn':
            name_arg_str = tree.args[0].as_string()
            col_arg_str = tree.args[1].as_string()
            new_arg_str, export_code = phi_col(col_arg_str, new_prev_df, globs, locs)
            code_export_list.extend(export_code.split('\n'))
            return f"{new_prev_df}.{structured_api}({name_arg_str}, {new_arg_str})", '\n'.join(code_export_list)
        elif structured_api == 'orderBy' or structured_api == 'orderby':
            new_arg_list = []
            if save_implicit:
                print(f"implicit flow ({structured_api}) at: {code}")
            for arg in tree.args:
                arg_str = arg.as_string()
                new_arg_str, export_code = phi_col(arg_str, new_prev_df, globs, locs)
                code_export_list.extend(export_code.split('\n'))
                # get value field
                value_type = get_value_type(new_arg_str, export_code, new_prev_df, globs, locs)
                code_export_list.extend(build_get_value_udf(value_type).split('\n'))
                arg_value_code = get_value_code(new_arg_str, value_type)
                new_arg_list.append(arg_value_code)
                if save_implicit:
                    save_cond(arg_str, '\n'.join(code_export_list), new_prev_df, globs, locs)
            return f"{new_prev_df}.{structured_api}({', '.join(new_arg_list)})", '\n'.join(code_export_list)
        elif structured_api == 'union':
            assert len(tree.args) == 1
            arg_str = tree.args[0].as_string()
            new_arg_str, export_code = phi_stmt(arg_str, arg_str, globs, locs)
            code_export_list.extend(export_code.split('\n'))
            return f"{new_prev_df}.{structured_api}({new_arg_str})", '\n'.join(code_export_list)
        elif structured_api == 'filter' or structured_api == 'where':
            assert len(tree.args) == 1
            arg_str = tree.args[0].as_string()
            new_arg_str, export_code = phi_cond(arg_str, new_prev_df, globs, locs)
            code_export_list.extend(export_code.split('\n'))
            if save_implicit:
                print(f"implicit flow ({structured_api}) at: {code}")
                save_cond(arg_str, '\n'.join(code_export_list), new_prev_df, globs, locs)
            return f"{new_prev_df}.{structured_api}({new_arg_str})", '\n'.join(code_export_list)
        elif structured_api == 'join':
            assert len(tree.args) > 1 # at least two args
            # args[0] is the df to join
            stmt_str = tree.args[0].as_string()
            new_stmt_str, export_code = phi_stmt(stmt_str, stmt_str, globs, locs)
            code_export_list.extend(export_code.split('\n'))
            tree.args[0] = astroid.parse(new_stmt_str).body[0]
            # args[1] is the condition to join on
            cond_arg = tree.args[1]
            arg_str = cond_arg.as_string()
            # change to a standard format
            if isinstance(cond_arg, astroid.nodes.Const):
                arg_str = f"{df}.{cond_arg.value} == {new_stmt_str}.{cond_arg.value}"
            new_arg_str, export_code = phi_cond(arg_str, new_prev_df, globs, locs)
            code_export_list.extend(export_code.split('\n'))
            tree.args[1] = astroid.parse(new_arg_str).body[0]
            tree.func.expr = astroid.parse(new_prev_df).body[0]
            return tree.as_string(), '\n'.join(code_export_list)
        elif structured_api == 'count' or structured_api == 'distinct':
            return f"{new_prev_df}.{structured_api}()", '\n'.join(code_export_list)
        elif structured_api == 'collect':
            code_export_list.append("from lib.taint_dataframe import untaint")
            return f"untaint({new_prev_df}).{structured_api}()", '\n'.join(code_export_list)
        else:
            print(f"[WARN] unsupported structured api: {structured_api}, try to skip.")
            return code, ""
    
    else:
        return code, ""


def phi_agg(code, df, globs, locs):
    tree = astroid.extract_node(code)
    code_export_list = []
    
    prev_df = tree.func.expr.as_string()
    new_prev_df, export_code = phi_stmt(prev_df, df, globs, locs)
    code_export_list.extend(export_code.split('\n'))
    tree.func.expr = astroid.extract_node(new_prev_df)

    agg_args = tree.args
    new_agg_args = []

    # process agg args
    for arg in agg_args:
        arg_code = arg.as_string()
        new_arg_code, export_code = phi_col(arg_code, new_prev_df, globs, locs)
        code_export_list.extend(export_code.split('\n'))
        new_arg = astroid.extract_node(new_arg_code)
        new_agg_args.append(new_arg)
    
    tree.args = new_agg_args
    return tree.as_string(), '\n'.join(code_export_list)

def phi_groupby_agg(code, df, globs, locs):
    from lib.phi.constant import gch
    tree = astroid.extract_node(code)
    code_export_list = []
    
    prev_df = tree.func.expr.func.expr.as_string()
    new_prev_df, export_code = phi_stmt(prev_df, df, globs, locs)
    code_export_list.extend(export_code.split('\n'))
    tree.func.expr.func.expr = astroid.extract_node(new_prev_df)
    
    groupby_node = tree.func.expr
    groupby_args = groupby_node.args
    new_groupby_args = []
    groupby_args_types = []
    agg_args = tree.args
    new_agg_args = []
    # process groupby args
    for i, arg in enumerate(groupby_args):
        arg_code = arg.as_string()
        gch.add_groupby_col(arg_code)
        new_arg_code, export_code = phi_col(arg_code, new_prev_df, globs, locs)
        code_export_list.extend(export_code.split('\n'))
        arg_value_type = get_value_type(new_arg_code, export_code, new_prev_df, globs, locs)
        groupby_args_types.append(arg_value_type)
        # value is in grouby
        code_export_list.extend(build_get_value_udf(arg_value_type).split('\n'))
        new_arg_value = astroid.extract_node(f"get_value_{arg_value_type}({new_arg_code}).alias('value_groupby_col_{i}')")
        new_groupby_args.append(new_arg_value)
        # tag is in agg
        from lib.phi.constant import tag_type
        code_export_list.extend(build_get_tag_udf(tag_type).split('\n'))
        code_export_list.extend(build_row_agg_pudf(tag_type).split('\n'))
        new_arg_tag = astroid.extract_node(f"row_agg({get_tag_code(new_arg_code)}).alias('tag_groupby_col_{i}')")
        new_agg_args.append(new_arg_tag)
    # process agg args
    for arg in agg_args:
        arg_code = arg.as_string()
        new_arg_code, export_code = phi_col(arg_code, new_prev_df, globs, locs)
        code_export_list.extend(export_code.split('\n'))
        new_arg = astroid.extract_node(new_arg_code)
        new_agg_args.append(new_arg)
    tree.func.expr.args = new_groupby_args
    tree.args = new_agg_args
    # repack
    repacked_cols = []
    droped_cols = []
    for i, arg in enumerate(groupby_args):
        col_type = groupby_args_types[i]
        code_export_list.extend(build_pack_udf(col_type).split('\n'))
        original_col_name = get_col_name(arg.as_string(), '', globs, locs)
        repacked_col_code = pack_code(col_type, f"'value_groupby_col_{i}'", f"'tag_groupby_col_{i}'")
        repacked_col_code += f".alias('{original_col_name}')"
        repacked_cols.append(repacked_col_code)
        # repacked_cols.append(f"pack_{col_type}('value_groupby_col_{i}', 'tag_groupby_col_{i}').alias('{original_col_name}')")
        droped_cols.extend([f"'value_groupby_col_{i}'", f"'tag_groupby_col_{i}'"])
    new_code = f"{tree.as_string()}.select({', '.join(repacked_cols)}, '*').drop({', '.join(droped_cols)})"
    gch.clear()
    return new_code, '\n'.join(code_export_list)


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
        # df.printSchema()
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

    # test withColumn and select
    code, export_code = phi_stmt("documents.withColumn('dup_author', length(col('Author'))).select('dup_author')", "documents", globals(), locals())
    print("old: documents.withColumn('dup_author', length(col('Author'))).select('dup_author')")
    print("new:", code)
    exec(f"""{export_code}
{code}.show()""")

    # test orderBy
    code, export_code = phi_stmt("documents.withColumn('dup_author', length(col('Author'))).select('dup_author', 'Author').orderBy('dup_author')", "documents", globals(), locals())
    # print(export_code, code)
    exec(f"""{export_code}
{code}.show()""")

    # test filter
    code, export_code = phi_stmt("documents.withColumn('dup_author', length(col('Author')))\
.filter((length(col('FileName')) > 10) & (col('FileName').endswith('pdf')))\
.select('dup_author', 'Author', 'FileName')\
.orderBy('dup_author')", "documents", globals(), locals())
    # print(export_code, code)
    exec(f"""{export_code}
{code}.show()""")

    # test join
    structureData = [
        ("Dan Zarzar","36636","M",3100),
        ("James","40288","M",4300),
        ("Robert","42114","M",1400),
        ("Maria","39192","F",5500),
        ("Jen","","F",-1)
    ]
    structureSchema = StructType([
        StructField('name', StringType(),True),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])
    documents_2 = taint_col(spark.createDataFrame(data=structureData,schema=structureSchema), "name")
    code, export_code = phi_stmt("documents.select('Author', 'FileName').join(documents_2, documents.Author == documents_2['name'])", "documents", globals(), locals())
    # print(code)
    exec(f"""{export_code}
{code}.show()""")

    # test union
    documents_3 = taint_col(spark.read.json(groundTruthDataFile), "Author")
    code, export_code = phi_stmt("documents.select('Author', 'FileName').filter(length(col('Author')) > 14).union(documents_3.select('Author', 'FileName').filter(length(col('Author')) < 14))", "documents", globals(), locals())
    # print(code)
    exec(f"""{export_code}
{code}.show()""")

    # test groupby
    code, export_code = phi_stmt("documents.withColumn('dup_author', length(col('Author'))).groupBy('Author', 'dup_author').agg(avg(length('FileName')))", "documents", globals(), locals())
    # print("old: documents.withColumn('dup_author', length(col('Author'))).groupBy('Author', 'dup_author').agg(avg(length('FileName')))")
    # print('\n')
    # print(f"new: {code}")
    exec(f"""{export_code}
{code}.show()""")
