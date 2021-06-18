# in this file, we provide some function to maintaint the dependency graph of each column
# e.g., df_1 = df.select("a") => we get a dependency that df_1.a <- [df.a]

import astroid
# from lib.phi.phi import seems_like_input_api, input_apis_meta, structured_apis
from lib.phi.util import *
from lib.dependency.analyze_dependency import analyze_dependency

dependency_graph = {} # key: index, value: dependency of a stmt

def get_input_path(code_node, globs, locs):
    from lib.phi.phi import input_apis_meta
    func_code = code_node.func.as_string()
    path_index = -1
    for input_api in input_apis_meta:
        if input_api in func_code:
            path_index = input_apis_meta[input_api]
    arg_code = code_node.args[path_index].as_string()
    return get_obj_from_code_str(arg_code, "", globs, locs)


def add_dependency(code, globs, locs):
    dependency_template = {
        "code": "",
        "source_df": [],
        "target_df": "",
        "dependencies": [], # list of dict: key: target column, value: [dependent columns]
        "conds": {}, # condistions in join, filter, where
        "is_input": False,
        "input_dir": "",
        "is_output": False
    }
    
    from lib.phi.phi import seems_like_input_api, structured_apis
    structured_apis.append("groupBy")
    new_dependency = dependency_template.copy()
    new_dependency["code"] = code
    
    tree = code2node(code)

    if isinstance(tree, astroid.nodes.Assign) and len(tree.targets) == 1:
        new_dependency["source_df"].append(tree.value.as_string())
        new_dependency["target_df"] = tree.targets[0].as_string()
        add_dependency(tree.value.as_string(), globs, locs)
        i = format(len(dependency_graph), '05d')
        dependency_graph[i] = new_dependency
    elif isinstance(tree, astroid.nodes.Call) and seems_like_input_api(tree.func.as_string()):
        new_dependency["source_df"].append("file")
        new_dependency["target_df"] = code
        new_dependency["is_input"] = True
        new_dependency["input_dir"] = get_input_path(tree, globs, locs)
        i = format(len(dependency_graph), '05d')
        dependency_graph[i] = new_dependency
    elif isinstance(tree, astroid.nodes.Call) and tree.func.attrname in structured_apis:
        prev_df_node = tree.func.expr
        prev_df = prev_df_node.as_string()
        if isinstance(prev_df_node, astroid.nodes.Call):
            add_dependency(prev_df, globs, locs)
        new_dependency["source_df"].append(prev_df)
        new_dependency["target_df"] = code
        if tree.func.attrname == "select":
            for arg in tree.args:
                col_name = get_col_name(arg.as_string(), '', globs, locs)
                if col_name == "unresolvedstar()":
                    new_dependency["dependencies"].append("one by one")
                else:
                    dependency = analyze_dependency(arg, globs, locs)
                    new_dependency["dependencies"].append({col_name: dependency})
        elif tree.func.attrname == "withColumn":
            arg_1 = tree.args[0]
            col_name = get_obj_from_code_str(arg_1.as_string(), "", globs, locs)
            dependency = analyze_dependency(tree.args[1], globs, locs)
            new_dependency["dependencies"].append({col_name: dependency})
            new_dependency["dependencies"].append("one by one") # onr by one means target df's col depend on source df's col one by one
        elif tree.func.attrname == "orderBy":
            for arg in tree.args:
                new_dependency["conds"][arg.as_string()] = "orderBy"
            new_dependency["dependencies"].append("one by one")
        elif tree.func.attrname == "where" or tree.func.attrname == "filter":
            for arg in tree.args:
                new_dependency["conds"][arg.as_string()] = "filter"
            new_dependency["dependencies"].append("one by one")
        elif tree.func.attrname == "union":
            new_dependency["source_df"].append(tree.args[0].as_string())
            new_dependency["dependencies"].append("one by one") # union is a one by one dependency
        elif tree.func.attrname == "join":
            new_dependency["source_df"].append(tree.args[0].as_string())
            new_dependency["conds"][tree.args[1].as_string()] = "join"
            new_dependency["dependencies"].append("one by one") 
        elif tree.func.attrname == "groupBy":
            new_dependency["dependencies"].append("one by one")
            for arg in tree.args:
                new_dependency["conds"][arg.as_string()] = "groupBy"
        elif tree.func.attrname == "agg":
            for arg in tree.args:
                col_name = get_col_name(arg.as_string(), '', globs, locs)
                dependency = analyze_dependency(arg, globs, locs)
                new_dependency["dependencies"].append({col_name: dependency})
        elif tree.func.attrname == "distinct":
            new_dependency["conds"]["all_col"] = "distinct"
        else: # count, collect
            new_dependency["is_output"] = True
        i = format(len(dependency_graph), '05d')
        dependency_graph[i] = new_dependency
    elif 'reduceByKey' in code and 'map' in code and tree.func.attrname == 'toDF':
        pass # TODO dependency in map reduce
    else:
        print("how???" + code)
        return 
    

def save_graph():
    import json
    f = open("dependency_graph.json", "w")
    json.dump(dependency_graph, f, indent=2)
    f.close()
