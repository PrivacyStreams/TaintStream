from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import *


def build_avg_pudf(return_type):
    from lib.phi.util import type_obj_to_code, format_type
    return f"""
@pandas_udf({type_obj_to_code(return_type)}, PandasUDFType.GROUPED_AGG)
def pudf_avg_{format_type(return_type)}(elements):
    import numpy as np
    return np.mean(elements)
"""

def build_max_pudf(return_type):
    from lib.phi.util import type_obj_to_code, format_type
    return f"""
@pandas_udf({type_obj_to_code(return_type)}, PandasUDFType.GROUPED_AGG)
def pudf_max_{format_type(return_type)}(elements):
    import numpy as np
    return np.max(elements)
"""

def build_min_pudf(return_type):
    from lib.phi.util import type_obj_to_code, format_type
    return f"""
@pandas_udf({type_obj_to_code(return_type)}, PandasUDFType.GROUPED_AGG)
def pudf_min_{format_type(return_type)}(elements):
    import numpy as np
    return np.min(elements)
"""

def build_sum_pudf(return_type):
    from lib.phi.util import type_obj_to_code, format_type
    return f"""
@pandas_udf({type_obj_to_code(return_type)}, PandasUDFType.GROUPED_AGG)
def pudf_sum_{format_type(return_type)}(elements):
    print(elements)
    import numpy as np
    print(np.sum(elements))
    return np.sum(elements)
"""

def build_count_pudf(return_type):
    from lib.phi.util import type_obj_to_code, format_type
    return f"""
@pandas_udf({type_obj_to_code(return_type)}, PandasUDFType.GROUPED_AGG)
def pudf_count_{format_type(return_type)}(elements):
    return len(elements)
"""

def build_stddev_pop_pudf(return_type):
    from lib.phi.util import type_obj_to_code, format_type
    # refer to http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.stddev_pop
    # refer to https://stackoverflow.com/questions/34050491/standard-deviation-in-numpy
    return f"""
@pandas_udf({type_obj_to_code(return_type)}, PandasUDFType.GROUPED_AGG)
def pudf_stddev_pop_{format_type(return_type)}(elements):
    import numpy as np
    return np.std(elements)
"""

def build_row_agg_pudf(tag_type):
# TODO - support more types of tag_agg
    if isinstance(tag_type, BooleanType):
        return f"" # function is implemented in lib/udf/row_agg.py
    else: 
        raise NotImplementedError(f"TaintStream do not support tag type of {tag_type}")


name2func = {
    'avg': build_avg_pudf,
    'max': build_max_pudf,
    'min': build_min_pudf,
    'sum': build_sum_pudf,
    'count': build_count_pudf,
    'stddev_pop': build_stddev_pop_pudf
}

name2returnType = {
    'avg': DoubleType(),
    'max': -1, # -1 for same as the input argument's type
    'min': -1,
    'sum': -1,
    'count': LongType(),
    'stddev_pop': DoubleType(),
    'first': -1,
    'mean': DoubleType(),
    'sqrt': DoubleType(),
    'collect_set': -1,
    'collect_list': -1,
    'explode': -1
}

# these aggregation functions  take a basic type (IntegerType/ShortType) and return a type (LongType) that is more precise 
type_extented_udaf = [
    'sum'
]

# aggregation function that returns an array type
return_array_udaf = [
    'collect_set',
    'collect_list',
    'explode'
]