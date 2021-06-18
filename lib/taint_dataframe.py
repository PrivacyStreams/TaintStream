from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.types import *
from pyspark.sql.functions import lit, udf, col, PandasUDFType
from pyspark.sql.column import Column, _to_java_column, _to_seq
from lib.phi.constant import TaintStreamError


def taint_col(df, col_name):
    for column in df.columns:
        dt = df.schema[column].dataType
        if isinstance(dt, TimestampType):
            # pyspark has problem when converting timestamps between builtin spark type and python type, just cast them to string
            # see details in https://stackoverflow.com/questions/50885719/pyspark-cant-do-column-operations-with-datetime-years-0001
            df = df.withColumn(column, df[column].cast("string"))
            dt = StringType()
        if isinstance(dt, StructType):
            tag = None
            if column == col_name:
                tag = lit(True)
            new_schema = StructType()
            pack_field_args = []
            for field in dt:
                if col_name == f"{column}.{field.name}":
                    tag = lit(True)
                else:
                    tag = lit(False)
                packed_schema = StructType([
                    StructField("value", field.dataType, field.nullable),
                    StructField("tag", BooleanType(), False)
                ])
                @udf(packed_schema)
                def pack(col_v, col_t):
                    return (col_v, col_t)
                pack_field_args.append(pack(col(f"{column}.{field.name}"), tag).alias(field.name))
                new_schema.add(StructField(
                    field.name,
                    packed_schema,
                    field.nullable
                ))
            @udf(new_schema)
            def pack_field(*cols):
                return cols
            df = df.withColumn(column, pack_field(*pack_field_args))
        else:
            packed_schema = StructType([
                StructField("value", dt, df.schema[column].nullable),
                StructField("tag", BooleanType(), False)
            ])
            @udf(packed_schema)
            def pack(col_v, col_t):
                return (col_v, col_t)
            if column == col_name:
                df = df.withColumn(column, pack(df[column], lit(True)))
            else:
                df = df.withColumn(column, pack(df[column], lit(False)))
    return df


def taint_cols(df, col_names):
    for column in df.columns:
        dt = df.schema[column].dataType
        if isinstance(dt, TimestampType):
            # pyspark has problem when converting timestamps between builtin spark type and python type, just cast them to string
            # see details in https://stackoverflow.com/questions/50885719/pyspark-cant-do-column-operations-with-datetime-years-0001
            df = df.withColumn(column, df[column].cast("string"))
            dt = StringType()
        if isinstance(dt, StructType):
            tag = None
            if column in col_names:
                tag = lit(True)
            new_schema = StructType()
            pack_field_args = []
            parent_tag = tag
            for field in dt:
                # process field tag
                if parent_tag is None:
                    tag = None
                    for col_name in col_names:
                        if col_name == f"{column}.{field.name}":
                            tag = lit(True)
                    if tag is None:
                        tag = lit(False)
                else:
                    tag = parent_tag
                # struct in a struct 
                if isinstance(field.dataType, StructType):
                    field_packed_schema = StructType()
                    pack_sub_field_args = []
                    for sub_field in field.dataType:
                        if isinstance(sub_field.dataType, StructType):
                            print("[TaintStream ERROR] TaintStream now only support two layers of structType, three or more detected.")
                            raise TaintStreamError('TaintStream now only support two layers of structType, got three or more')
                        packed_schema = StructType([
                            StructField("value", sub_field.dataType, sub_field.nullable),
                            StructField("tag", BooleanType(), False)
                        ])
                        @udf(packed_schema)
                        def pack(col_v, col_t):
                            return (col_v, col_t)
                        pack_sub_field_args.append(pack(
                            col(f"{column}.{field.name}.{sub_field.name}"),
                            tag
                        ).alias(sub_field.name))
                        field_packed_schema.add(StructField(
                            sub_field.name,
                            packed_schema,
                            sub_field.nullable
                        ))
                    @udf(field_packed_schema)
                    def pack_sub_field(*cols):
                        return cols
                    pack_field_args.append(pack_sub_field(*pack_sub_field_args))
                    new_schema.add(StructField(
                        field.name,
                        field_packed_schema,
                        field.nullable
                    ))
                else: # basic in a struct    
                    packed_schema = StructType([
                        StructField("value", field.dataType, field.nullable),
                        StructField("tag", BooleanType(), False)
                    ])
                    @udf(packed_schema)
                    def pack(col_v, col_t):
                        return (col_v, col_t)
                    pack_field_args.append(pack(col(f"{column}.{field.name}"), tag).alias(field.name))
                    new_schema.add(StructField(
                        field.name,
                        packed_schema,
                        field.nullable
                    ))
            @udf(new_schema)
            def pack_field(*cols):
                return cols
            df = df.withColumn(column, pack_field(*pack_field_args))
        else:
            packed_schema = StructType([
                StructField("value", dt, df.schema[column].nullable),
                StructField("tag", BooleanType(), False)
            ])
            @udf(packed_schema)
            def pack(col_v, col_t):
                return (col_v, col_t)
            if column in col_names:
                df = df.withColumn(column, pack(df[column], lit(True)))
            else:
                df = df.withColumn(column, pack(df[column], lit(False)))
    return df
        

def untaint(df):
    from pyspark import RDD 
    if isinstance(df, RDD):
        return df
    value_cols = [] # to select
    for column in df.columns:
        dt = df.schema[column].dataType
        if not isinstance(dt, StructType):
            raise TypeError(f"fail to untaint on column {column} with data type of {dt}. please contact the developer of TaintStream.")
        col_field_names = dt.fieldNames()
        if len(col_field_names) == 2 and "tag" in col_field_names and "value" in col_field_names:
            @udf(dt["value"].dataType)
            def get_value(col):
                return col[0]
            value_cols.append(col(f"`{column}`.`value`").alias(column))
        else: # original column is a structured type
            original_schema = restore_structType(dt)
            @udf(original_schema)
            def repack_col(*cols):
                return cols
            original_col_names = original_schema.fieldNames()
            repack_arg_list = []
            for col_name in original_col_names:
                @udf(original_schema[col_name].dataType)
                def get_field_value(col):
                    try:
                        return col[0]
                    except:
                        return None
                repack_arg_list.append(get_field_value(col(f"`{column}`.`{col_name}`.`value`")))
            value_cols.append(repack_col(*repack_arg_list).alias(column))
    return df.select(*value_cols)
    



def restore_structType(schema):
    ret = StructType()
    for field in schema:
        ret.add(field.name, field.dataType["value"].dataType, field.dataType["value"].nullable)
    return ret


def build_repack(i, schema_code):
    return f"""
@udf({schema_code})
def repack_col_{i}(*cols):
    return cols
"""


def cal_table_tag(df):
    def row_agg(col):
        sc = SparkSession.builder.getOrCreate().sparkContext
        _f = sc._jvm.com.microsoft.udaf.RowAggBoolean.apply
        return Column(_f(_to_seq(sc,[col], _to_java_column)))
    
    @udf(BooleanType())
    def col_agg(*tags):
        ret = False
        for tag in tags:
            ret = tag | ret
        return ret
    value_cols = []
    for column in df.columns:
        dt = df.schema[column].dataType
        if not isinstance(dt, StructType):
            raise TypeError(f"fail to untaint on column {column} with data type of {dt}. please contact the developer of TaintStream.")
        col_field_names = dt.fieldNames()
        if len(col_field_names) == 2 and "tag" in col_field_names and "value" in col_field_names:
            value_cols.append(col(f"{column}.tag").alias(column))
        else: # original column is a structured type
            col_field_names = dt.fieldNames()
            repack_arg_list = [col(f"{column}.{col_name}.tag") for col_name in col_field_names]
            value_cols.append(col_agg(*repack_arg_list).alias(column))
    tag = df.agg(row_agg(col_agg(*value_cols)).alias('tag')).collect()
    return tag[0]['tag']


def taint_table(df, tag_value):
    tag = lit(tag_value)
    for column in df.columns:
        dt = df.schema[column].dataType
        if isinstance(dt, TimestampType):
            # pyspark has problem when converting timestamps between builtin spark type and python type, just cast them to string
            # see details in https://stackoverflow.com/questions/50885719/pyspark-cant-do-column-operations-with-datetime-years-0001
            df = df.withColumn(column, df[column].cast("string"))
            dt = StringType()
        if isinstance(dt, StructType):
            new_schema = StructType()
            pack_field_args = []
            for field in dt:
                packed_schema = StructType([
                    StructField("value", field.dataType, field.nullable),
                    StructField("tag", BooleanType(), False)
                ])
                @udf(packed_schema)
                def pack(col_v, col_t):
                    return (col_v, col_t)
                pack_field_args.append(pack(col(f"{column}.{field.name}"), tag).alias(field.name))
                new_schema.add(StructField(
                    field.name,
                    packed_schema,
                    field.nullable
                ))
            @udf(new_schema)
            def pack_field(*cols):
                return cols
            df = df.withColumn(column, pack_field(*pack_field_args))
        else:
            packed_schema = StructType([
                StructField("value", dt, df.schema[column].nullable),
                StructField("tag", BooleanType(), False)
            ])
            @udf(packed_schema)
            def pack(col_v, col_t):
                return (col_v, col_t)
            df = df.withColumn(column, pack(df[column], tag))
    return df


def test():
    import pyspark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[1]") \
                        .appName('SparkByExamples.com') \
                        .getOrCreate()

    # test basic column
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

    df = spark.createDataFrame(data=structureData_notaint,schema=structureSchema_notaint)
    df.printSchema()
    df.show(truncate=False)

    dft = taint_col(df, "unexist_col")
    dft.printSchema()
    dft.show(truncate=False)
    
    tag = cal_table_tag(dft)
    df = untaint(dft)
    df.printSchema()
    df.show(truncate=False)
    dft = taint_table(df, tag)
    dft.printSchema()
    dft.show(truncate=False)
    
    # test structured column
    structureData_notaint = [
        ("James","M",(3100,20)),
        ("Michael","M",(4300,20)),
        ("Robert","M",(1400,21)),
        ("Maria","F",(5500,20)),
        ("Jen","F",(0,20))
    ]
    structureSchema_notaint = StructType([
            StructField('name', StringType(), True),
            StructField('gender', StringType(), True),
            StructField(
                'property', 
                StructType([
                    StructField('salary', IntegerType(), True),
                    StructField('age', IntegerType(), True),
                ]), 
                True
            )
            ])

    df = spark.createDataFrame(data=structureData_notaint,schema=structureSchema_notaint)
    df.printSchema()
    df.show(truncate=False)

    dft = taint_col(df, "name")
    dft.printSchema()
    dft.show(truncate=False)
    
    tag = cal_table_tag(dft)
    df = untaint(dft)
    df.printSchema()
    df.show(truncate=False)
    dft = taint_table(df, tag)
    dft.printSchema()
    dft.show(truncate=False)
    