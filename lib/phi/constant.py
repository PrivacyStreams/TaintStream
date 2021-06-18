from pyspark.sql.types import *

tag_type = BooleanType()
type2default = {
    BooleanType(): False
}

class taint_col_handler():
    tainted_cols = []
    
    @classmethod
    def get_tainted_cols(cls):
        return cls.tainted_cols
    
    @classmethod
    def add_tainted_col(cls, taint_col_name):
        cls.tainted_cols.append(taint_col_name)
    
    @classmethod
    def extend_tainted_cols(cls, taint_col_name_list):
        cls.tainted_cols.extend(taint_col_name_list)

save_implicit = True

# Exception
class TaintStreamError(Exception):
    pass

class RewriteError(TaintStreamError):
    pass

class ExecutionError(TaintStreamError):
    pass

# record the current groupby column
class GroupByColHandler():
    groupby_cols = []

    @classmethod
    def get_groupby_cols(cls):
        return cls.groupby_cols
    
    @classmethod
    def add_groupby_col(cls, groupby_col_name):
        cls.groupby_cols.append(groupby_col_name)
    
    @classmethod
    def clear(cls):
        cls.groupby_cols = []

gch = GroupByColHandler()