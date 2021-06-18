import astroid
from pyspark.sql.types import *
from pyspark.sql.functions import *
from lib.phi.util import *
from lib.phi.constant import *
from lib.phi.phi_stmt import phi_stmt
from pyspark.sql import Row
import re


# we assert that all the map reduce is following the format like this:
# df.rdd.map(lambda x: (expression of x, expression of x)).reduceByKey(lambda a, b: expression of a, b).toDF(schema)
# e.g., df.rdd.map(lambda x: (x[1], '&'.join([x[0], x[2]]))).reduceByKey(lambda a, b: str(a) + '#' + str(b)).toDF(schema=schema)
# e.g., df.rdd.map(lambda x: ((x[0], x[1]), [x[2]])).reduceByKey(lambda a, b: a + b).toDF()
def phi_mapreduce(code, df, globs, locs):
    # globs.update(locs)
    tree = astroid.extract_node(code)
    code_export_list = []
    assert isinstance(tree, astroid.nodes.Call)

    prev_df = tree.func.expr.func.expr.func.expr.expr.as_string()
    new_prev_df, export_code = phi_stmt(prev_df, df, globs, locs)
    code_export_list.extend(export_code.split('\n'))
    tree.func.expr.func.expr.func.expr.expr = astroid.extract_node(new_prev_df)

    output_schema_tree = get_output_schema_tree(tree)
    if output_schema_tree == None:
        output_schema, export_code = inference_schema(tree, export_code, globs, locs)
        code_export_list.extend(export_code.split('\n'))
        output_schema_tree = astroid.extract_node("output_schema")
    else:
        output_schema = get_obj_from_code_str(output_schema_tree.as_string(), '', globs, locs)
    new_output_schema = construct_schema(output_schema)

    # print(map_func_tree.repr_tree(), map_func_tree.as_string())
    # print(reduce_func_tree.repr_tree(), reduce_func_tree.as_string())
    # print(output_schema_tree.repr_tree(), output_schema_tree.as_string())

    # extract map function, reduce function, and output schema
    map_func_tree = uniform_map_tree(get_map_func_tree(tree))
    reduce_func_tree = uniform_reduce_tree(get_reduce_func_tree(tree))
    assert isinstance(map_func_tree, astroid.nodes.Lambda) and isinstance(reduce_func_tree, astroid.nodes.Lambda)
    
    if support_field_sensitive(map_func_tree):
        code_export_list.extend(helper_functions().split('\n'))
        # 1. key of the map function
        map_keys = map_func_tree.body.elts[0]
        map_arg = map_func_tree.args.args[0].name
        new_map_keys = astroid.nodes.Tuple()
        ker_recorder = [] # record each element in the key use which column, e.g., for (x[1]+x[2], x[3]), ker_recorder = [[x[1], x[2]], [x[3]]]
        for key in map_keys.elts:
            # find and replace all x[]...
            key_code = key.as_string()
            cols = re.findall(map_arg+r"\[\d+\]", key_code)
            ker_recorder.append(cols)
            new_key_code = re.sub(map_arg+r"\[\d+\]", r"get_value_mr(\g<0>)", key_code)
            new_map_keys.elts.append(astroid.extract_node(new_key_code))
        # do not use tuple if only key onely has one element
        if len(new_map_keys.elts) == 1:
            new_map_keys = new_map_keys.elts[0]
        map_func_tree.body.elts[0] = new_map_keys

        # 2. value of the map function
        map_values = map_func_tree.body.elts[1]
        new_map_values = astroid.nodes.Tuple()
        # tag of the key elements
        for cols in ker_recorder:
            if len(cols) == 1:
                new_value = astroid.extract_node(f"get_tag_mr({cols[0]})")
            else:
                value_list = [f"get_tag_mr({col})" for col in cols]
                new_value = astroid.extract_node(f"col_agg_mr({', '.join(value_list)})")
            new_map_values.elts.append(new_value)
        # value and tag of the value elements
        for value in map_values.elts:
            value_code = value.as_string()
            cols = re.findall(map_arg+r"\[\d+\]", value_code)
            new_value_code = re.sub(map_arg+r"\[\d+\]", r"get_value_mr(\g<0>)", value_code)
            # value of the value element
            new_map_values.elts.append(astroid.extract_node(new_value_code))
            # tag of the value element
            if len(cols) == 0:
                # no column in this value element
                new_map_values.elts.append(astroid.extract_node("tag_of_lit_mr()"))
            elif len(cols) == 1:
                new_map_values.elts.append(astroid.extract_node(f"get_tag_mr({cols[0]})"))
            else:
                value_list = [f"get_tag_mr({col})" for col in cols]
                new_map_values.elts.append(astroid.extract_node(f"col_agg_mr({', '.join(value_list)})"))
        if len(new_map_values.elts) == 1:
            new_map_values = new_map_values.elts[0]
        map_func_tree.body.elts[1] = new_map_values
        # print(map_func_tree.as_string())
    
        # 3. recduce function
        key_num = len(ker_recorder)
        value_num = len(new_map_values.elts)
        reduce_arg_0 = reduce_func_tree.args.args[0].name # typically, a
        reduce_arg_1 = reduce_func_tree.args.args[1].name # typically, b
        reduce_body = reduce_func_tree.body
        new_reduce_body = astroid.nodes.Tuple()
        for i in range(value_num):
            if i < key_num: # tag of the key element
                new_reduce_body.elts.append(astroid.extract_node(f"row_agg_mr({reduce_arg_0}[{i}], {reduce_arg_1}[{i}])"))
            elif (i-key_num)%2 == 0: # value of the value element
                origin_i = (i-key_num)//2
                original_code = reduce_body.elts[origin_i].as_string()
                new_code = original_code.replace(str(origin_i), str(i))
                new_reduce_body.elts.append(astroid.extract_node(new_code))
            else: # tag of the value element
                new_reduce_body.elts.append(astroid.extract_node(f"row_agg_mr({reduce_arg_0}[{i}], {reduce_arg_1}[{i}])"))
        reduce_func_tree.body = new_reduce_body
        # print(reduce_func_tree.as_string())

        # 4. put results of 2,3 together, then pack the value and tag of the key element
        tree.args = []
        tree.keywords = None # remove schema, because we will rebuild it later. 
        tree.func.expr.args[0] = reduce_func_tree # rebuild reduce function
        tree.func.expr.func.expr.args[0] = map_func_tree # rebuild map function
        value_num = (value_num-key_num)//2
        code_export_list.extend(build_pack_key_and_value_udf(output_schema_tree.as_string(), key_num, value_num).split('\n'))
        # build key
        
        assert key_num > 0 and value_num > 0
        if key_num == 1:
            pack_key_code = f"pack_key_0(col('_1'), col('_2._1'))"
        else:
            pack_key_code_args = []
            for i in range(key_num):
                pack_key_code_args.append(f"pack_key_{i}(col('_1._{i+1}'), col('_2._{i+1}'))")
            pack_key_code = f"pack_key({', '.join(pack_key_code_args)})"
        pack_key_code += f".alias('{new_output_schema[0].name}')" # restore column name
        # build value
        if value_num == 1:
            true_i = key_num+1
            pack_value_code = f"pack_value_0(col('_2._{true_i}'), col('_2._{true_i+1}'))"
        else:
            pack_value_code_args = []
            for i in range(value_num):
                true_i = key_num+2*i+1
                pack_value_code_args.append(f"pack_value_{i}(col('_2._{true_i}'), col('_2._{true_i+1}'))")
            pack_value_code = f"pack_value({', '.join(pack_value_code_args)})"
        pack_value_code += f".alias('{new_output_schema[1].name}')" # restore column name
        code = tree.as_string() + f".select({', '.join([pack_key_code, pack_value_code])})"


    else:
        raise TaintStreamError('TODO - support general formate of map reduce.')
    
    return code, '\n'.join(code_export_list)
     

def get_map_func_tree(tree):
    tree = tree.func.expr.func.expr
    func_name = get_func_name(tree)
    assert func_name == 'map'
    args = tree.args
    assert len(args) == 1 and isinstance(args[0], astroid.nodes.Lambda)
    return args[0]


def get_reduce_func_tree(tree):
    tree = tree.func.expr
    func_name = get_func_name(tree)
    assert func_name == 'reduceByKey'
    args = tree.args
    assert len(args) == 1 and isinstance(args[0], astroid.nodes.Lambda)
    return args[0]


def get_output_schema_tree(tree):
    func_name = get_func_name(tree)
    assert func_name == 'toDF'
    args = tree.args
    
    if len(args) > 0:
        return args[0]
    elif tree.keywords != None and len(tree.keywords) > 0:
        for keyword in tree.keywords:
            if keyword.arg == 'schema':
                return keyword.value
    else:
        return None


# we can support field-sensitive for map reduce when it follow the following format:
# key and value are tuple and the element of the tuple is x with index
# e.g., map function: lambda x: ((x[1], x[2], ...), (x[3], x[4], [x[5]], func(x[6]), ...))
# reduce function: lambda a,b: (a[0] + b[1], a[2] - b[3])
def support_field_sensitive(map_func_tree):
    # check key
    map_body = map_func_tree.body
    if not isinstance(map_body, astroid.nodes.Tuple) or len(map_body.elts) != 2:
        return False
    map_key = map_body.elts[0]
    if not isinstance(map_key, astroid.nodes.Tuple):
        return False
    arg = map_func_tree.args.args[0].name
    for e in map_key.elts:
        if not re.search(arg + r"\[\d+\]", e.as_string()) and not isinstance(e, astroid.nodes.Const):
            return False
    # check value
    map_value = map_body.elts[1]
    if not isinstance(map_value, astroid.nodes.Tuple):
        return False
    for e in map_value.elts:
        if not re.search(arg + r"\[\d+\]", e.as_string()) and not isinstance(e, astroid.nodes.Const):
            return False
    return True

# try to convert key and value to a standard tuple format
# e.g., map function: lambda x: (x[0], x[1])
# ===> map function: lambda x: ((x[0]), (x[1]))
def uniform_map_tree(map_func_tree):
    if not isinstance(map_func_tree, astroid.nodes.Lambda):
        # not a standard format, pass
        return map_func_tree
    map_body = map_func_tree.body
    if not isinstance(map_body, astroid.nodes.Tuple) or len(map_body.elts) != 2:
        # not a standard format, pass
        return map_func_tree
    new_body = astroid.nodes.Tuple()
    for e in map_body.elts:
        if isinstance(e, astroid.nodes.Tuple):
            # no need to convert
            new_body.elts.append(e)
        else:
            # convert 
            new_e = astroid.nodes.Tuple()
            new_e.postinit([e])
            new_body.elts.append(new_e)
    map_func_tree.body = new_body
    return map_func_tree


def uniform_reduce_tree(reduce_func_tree):
    if not isinstance(reduce_func_tree, astroid.nodes.Lambda):
        # not a standard format, pass
        return reduce_func_tree
    reduce_body = reduce_func_tree.body
    reduce_arg_0 = reduce_func_tree.args.args[0].name # typically, a
    reduce_arg_1 = reduce_func_tree.args.args[1].name # typically, b
    if not isinstance(reduce_body, astroid.nodes.Tuple):
        # not that here we should change all a/b in the code to a[0] b[0] and build a tuple in the body
        # use astroid instead of regex here, because regex might get a wrong anwser.
        def transform(node):
            ret = astroid.nodes.Subscript()
            _slice = astroid.nodes.Index()
            _slice.postinit(value=astroid.nodes.Const(0))
            ret.postinit(
                value= node,
                slice= _slice
            )
            return ret
        def need_transform(node):
            return node.name == reduce_arg_0 or node.name == reduce_arg_1
        astroid.MANAGER.register_transform(
            astroid.Name,
            transform,
            need_transform
        )
        reduce_body = astroid.extract_node(reduce_body.as_string())
        astroid.MANAGER.unregister_transform(
            astroid.Name,
            transform,
            need_transform
        )
        new_body = astroid.nodes.Tuple()
        new_body.postinit([reduce_body])
        reduce_func_tree.body = new_body
    return reduce_func_tree
    

def helper_functions():
    return """
from lib.phi.phi_mapreduce import *
from pyspark.sql import Row
import re

def get_value_mr(a):
    if isinstance(a, Row):
        return a[0]
    else: 
        return a

def get_tag_mr(b):
    if isinstance(b, Row):
        return b[1]
    else:
        return b

def row_agg_mr(a,b):
    return a|b

def col_agg_mr(*ts):
    ret = False
    for t in ts:
        ret |= t
    return ret

def tag_of_lit_mr():
    return False
"""


def build_pack_key_and_value_udf(original_schema_code, key_num, value_num):
    export_code =  f"""
new_schema = construct_schema({original_schema_code})
key_schema = new_schema[0].dataType
value_schema = new_schema[1].dataType

@udf(key_schema)
def pack_key(*cols):
    return cols

@udf(value_schema)
def pack_value(*cols):
    return cols

"""
    if key_num == 1:
        export_code += f"""
@udf(key_schema)
def pack_key_0(a, b):
    return (a, b)

"""
    else:
        for i in range(key_num):
            export_code += f"""
@udf(key_schema[{i}].dataType)
def pack_key_{i}(a, b):
    return (a, b)

"""
    if value_num == 1:
        export_code += f"""
@udf(value_schema)
def pack_value_0(a, b):
    return (a, b)
    
"""
    else:
        for i in range(value_num):
            export_code += f"""
@udf(value_schema[{i}].dataType)
def pack_value_{i}(a, b):
    return (a, b)

"""
    return export_code


def construct_schema(origin_schema):
    ret = StructType()
    if not len(origin_schema) == 2:
        raise TaintStreamError(
            f"error: output schema should have and only have 2 fields (key, value). Got {len(origin_schema)} instead."
        )
    for i, _ in enumerate(origin_schema):
        taint_schema = StructType()
        if not isinstance(_.dataType, StructType): # len(key) == 1 or len(value) == 1
            taint_schema.add("value", _.dataType, _.nullable, _.metadata)
            taint_schema.add("tag", tag_type, True)
        else:
            for field in _.dataType:
                if isinstance(field, StructType):
                    raise NotImplementedError("TODO - support StructType in map reduce")
                else:
                    taint_schema.add(
                        field.name, 
                        StructType([
                            StructField("value", field.dataType, field.nullable, field.metadata),
                            StructField("tag", tag_type, True)
                        ])
                    )
        ret.add(_.name, taint_schema)
    return ret


def inference_schema(tree, export_code, globs, locs):
    temp_tree = astroid.extract_node(tree.as_string())
    exec(export_code, globs, locs)
    export_code += "\nfrom lib.taint_dataframe import untaint"
    prev_df_code = temp_tree.func.expr.func.expr.func.expr.expr.as_string()
    temp_tree.func.expr.func.expr.func.expr.expr = astroid.extract_node(f"untaint({prev_df_code})")
    schema_code = f"{temp_tree.as_string()}.schema"
    schema_obj = get_obj_from_code_str(schema_code, export_code, globs, locs)
    extra_export_code = f"from lib.taint_dataframe import untaint\noutput_schema = {schema_code}"
    return schema_obj, extra_export_code



def test():
    import pyspark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[1]") \
                        .appName('SparkByExamples.com') \
                        .getOrCreate()

    structureData_notaint = [
        ("James","M",3100,20),
        ("Michael","M",4300,20),
        ("Robert","M",1400,21),
        ("Maria","F",5500,20),
        ("Jen","F",0,20)
    ]
    structureSchema_notaint = StructType([
            StructField('name', StringType(), True),
            StructField('gender', StringType(), True),
            StructField('salary', IntegerType(), True),
            StructField('age', IntegerType(), True),
            ])

    structureData = [
        (("James",True),("M", True),(3100,False),(20, False)),
        (("Michael",True),("M", True),(4300,False),(20, False)),
        (("Robert",True),("M", True),(1400,False),(21, False)),
        (("Maria",True),("F", True),(5500,False),(20, False)),
        (("Jen",True),("F", True),(0,False),(20, False))
    ]
    structureSchema = StructType([
            StructField('name', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
                ])),
            StructField('gender', StructType([
                StructField('value', StringType(), True),
                StructField('tag', BooleanType(), True)
                ])),
            StructField('salary', StructType([
                StructField('value', IntegerType(), True),
                StructField('tag', BooleanType(), True)
                ])),
            StructField('age', StructType([
                StructField('value', IntegerType(), True),
                StructField('tag', BooleanType(), True)
                ])),
            ])

    output_schema = StructType([
        StructField('_1', StructType([
            StructField('gender', StringType(), True),
            StructField('age', IntegerType(), True),
        ])),
        StructField('names', StringType(), True),
    ])

    df = spark.createDataFrame(data=structureData_notaint,schema=structureSchema_notaint)
    df.printSchema()
    df.show(truncate=False)

    df = df.rdd.map(lambda x: ((x[1], x[3]), x[0])).reduceByKey(lambda a, b: '&'.join([a, b])).toDF()
    df.printSchema()
    print("df.rdd.map(lambda x: ((x[1], x[3]), x[0])).reduceByKey(lambda a, b: '&'.join([a, b])).toDF()")
    df.show(truncate=False)


    df = spark.createDataFrame(data=structureData,schema=structureSchema)
    tmp_locals = locals().copy()
    tmp_globals = globals().copy()
    tmp_globals.update(tmp_locals)
    new_code, export_code = phi_mapreduce("df.rdd.map(lambda x: ((x[1], x[3]), x[0])).reduceByKey(lambda a, b: '&'.join([a, b])).toDF()", "df", tmp_globals, tmp_locals)

    # print(export_code)
    print(new_code)
    exec(f"{export_code}", tmp_globals, tmp_locals)
    tmp_globals.update(tmp_locals)
    exec(f"{new_code}.show(truncate=False)", tmp_globals, tmp_locals)

    output_schema = StructType([
        StructField('gender', StringType(), True),
        StructField('count', StringType(), True),
    ])

    tmp_locals = locals().copy()
    tmp_globals = globals().copy()
    tmp_globals.update(tmp_locals)
    new_code, export_code = phi_mapreduce("df.rdd.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a+b).toDF()", "df", tmp_globals, tmp_locals)
        
    # print(export_code)
    print(new_code)
    exec(f"{export_code}", tmp_globals, tmp_locals)
    tmp_globals.update(tmp_locals)
    exec(f"{new_code}.show(truncate=False)", tmp_globals, tmp_locals)

    output_schema = StructType([
        StructField('keys', StructType([
            StructField('gender', StringType(), True),
            StructField('age', IntegerType(), True),
        ])),
        StructField('values', StructType([
            StructField('max(salary)', IntegerType(), True),
            StructField('count', IntegerType(), True),
            StructField('name_list', ArrayType(StringType()), True),
        ])),
    ])

    def my_max(a, b):
        if a >= b: 
            return a
        else:
            return b
    tmp_locals = locals().copy()
    tmp_globals = globals().copy()
    tmp_globals.update(tmp_locals)
    new_code, export_code = phi_mapreduce("df.rdd.map(lambda x: ((x[1], x[3]), (x[2], 1, [x[0]]))).reduceByKey(lambda a, b: (my_max(a[0], b[0]), a[1] + b[1], a[2] + b[2])).toDF()", "df", tmp_globals, tmp_locals)
        
    # print(export_code)
    print(new_code)
    # tmp_locals = locals().copy()
    # tmp_globals = globals().copy()
    exec(f"{export_code}", tmp_globals, tmp_locals)
    tmp_globals.update(tmp_locals)
    exec(f"{new_code}.show(truncate=False)", tmp_globals, tmp_locals)
    