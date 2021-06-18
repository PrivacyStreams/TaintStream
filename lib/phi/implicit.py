import astroid
from pyspark.sql.types import *
from pyspark.sql.functions import *
from lib.phi.util import *
from lib.phi.constant import *
from lib.phi.phi_col import *
from lib.phi.phi_cond import *

# save column level implicit flow
def save_cond(code, export_code, df, globs, locs):
    return 
    exec(export_code, globs, locs)
    new_code, export_code = phi_col(code, df, globs, locs)
    from lib.phi.pudf import build_row_agg_pudf
    export_code = export_code + "\n" + build_get_tag_udf(tag_type)
    export_code = export_code + "\n" + build_row_agg_pudf(tag_type)
    exec(export_code, globs, locs)
    # tag_res = eval(f"{df}.groupBy(lit('__helper_col')).agg(row_agg(get_tag({new_code}))).drop('__helper_col').collect()", globs, locs)
    tag_res = eval(f"{df}.agg(row_agg({get_tag_code(new_code)})).collect()", globs, locs)
    print(f"aggregation of the tag of the involved cols ({code}): {tag_res[0][0]}")
