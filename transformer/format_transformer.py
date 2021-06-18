# this file contains the transformer that halps to format the code
# we need to reformate some code to a standard format to perform code rewrite.
# for example, we need to reformate "a = b if cond else c" to the following format
# if cond:
#     a = b
# else:
#     a = c

import astroid


def assign_list_comprehension_transform(node: astroid.nodes.Assign):
    ret = astroid.nodes.If()
    ifexp = node.value
    test = ifexp.test
    body = [astroid.nodes.Assign()]
    body[0].postinit(targets=node.targets, value=ifexp.body)
    orelse = [astroid.nodes.Assign()]
    orelse[0].postinit(targets=node.targets, value=ifexp.orelse)
    ret.postinit(test=test, body=body, orelse=orelse)
    return ret

def seems_like_custom_assign_list_comprehension(node):
    return isinstance(node.value, astroid.nodes.ListComp)

def return_list_comprehension_transform(node: astroid.nodes.Assign):
    ret = astroid.nodes.If()
    ifexp = node.value
    test = ifexp.test
    body = [astroid.nodes.Assign()]
    body[0].postinit(targets=node.targets, value=ifexp.body)
    orelse = [astroid.nodes.Assign()]
    orelse[0].postinit(targets=node.targets, value=ifexp.orelse)
    ret.postinit(test=test, body=body, orelse=orelse)
    return ret

def return_like_custom_assign_list_comprehension(node):
    return isinstance(node.value, astroid.nodes.ListComp)


def assign_transform(node: astroid.nodes.Assign):
    ret = astroid.nodes.If()
    ifexp = node.value
    test = ifexp.test
    body = [astroid.nodes.Assign()]
    body[0].postinit(targets=node.targets, value=ifexp.body)
    orelse = [astroid.nodes.Assign()]
    orelse[0].postinit(targets=node.targets, value=ifexp.orelse)
    ret.postinit(test=test, body=body, orelse=orelse)
    return ret

def seems_like_custom_assign(node):
    return isinstance(node.value, astroid.nodes.IfExp)


def return_transform(node: astroid.nodes.Assign):
    ret = astroid.nodes.If()
    ifexp = node.value
    test = ifexp.test
    body = [astroid.nodes.Return()]
    body[0].postinit(value=ifexp.body)
    orelse = [astroid.nodes.Return()]
    orelse[0].postinit(value=ifexp.orelse)
    ret.postinit(test=test, body=body, orelse=orelse)
    return ret

def seems_like_custom_return(node):
    return isinstance(node.value, astroid.nodes.IfExp)


def multilineblock_list_comp_transform(block_node):
    new_body = []
    for node in block_node.body:
        if isinstance(node, astroid.nodes.Return) and isinstance(node.value, astroid.nodes.ListComp):
            # return [a if b else c for d in e]
            # can there be two or more generators?
            new_code = f"""
ret = []
for {node.value.generators[0].target.as_string()} in {node.value.generators[0].iter.as_string()}:
    temp_arg = {node.value.elt.as_string()}
    ret.append(temp_arg)
return ret
"""
        elif isinstance(node, astroid.nodes.Assign) and isinstance(node.value, astroid.nodes.ListComp):
            # a = [b if e else d for e in f]
            new_code = f"""
_ret_ = []
for {node.value.generators[0].target.as_string()} in {node.value.generators[0].iter.as_string()}:
    temp_arg = {node.value.elt.as_string()}
    _ret_.append(temp_arg)
{node.targets[0].as_string()} = _ret_
"""
        else:
            new_code = node.as_string()
        new_node = astroid.parse(new_code)
        new_body.extend(new_node.body)
    block_node.body = new_body
    return block_node

def seems_like_custom_multilineblock_list_comp(block_node):
    for node in block_node.body:
        if isinstance(node, astroid.nodes.Return) and isinstance(node.value, astroid.nodes.ListComp):
            return True
        if isinstance(node, astroid.nodes.Assign) and isinstance(node.value, astroid.nodes.ListComp):
            return True
    return False

# user_uniform_avg_token_len = df_per_user.agg(sf.avg(df_per_user.token_count_avg)).collect()[0][0]
# ====>
# user_uniform_avg_token_len = df_per_user.agg(sf.avg(df_per_user.token_count_avg)).collect()
# user_uniform_avg_token_len = user_uniform_avg_token_len[0][0]
def multilineblock_subscript_transform(block_node):
    new_body = []
    for node in block_node.body:
        if isinstance(node, astroid.nodes.Return) \
            and isinstance(node.value, astroid.nodes.Subscript) \
                and child_has_call(node):
            call_node = get_call_node_from_subscript(node)
            ret_assign_code = f"ret = {call_node.as_string()}"
            new_body.append(astroid.extract_node(ret_assign_code))
            return_code = node.as_string()
            # use builtin 'replace' function here for convenience, can be changed to astroid transform
            return_code = return_code.replace(call_node.as_string(), "ret")
            new_body.append(astroid.extract_node(return_code))
        elif isinstance(node, astroid.nodes.Assign) \
            and isinstance(node.value, astroid.nodes.Subscript) \
                and child_has_call(node):
            assert len(node.targets) == 1
            target = node.targets[0]
            call_node = get_call_node_from_subscript(node)
            temp_assign_code = f"{target.as_string()} = {call_node.as_string()}"
            new_body.append(astroid.extract_node(temp_assign_code))
            assign_code = node.as_string()
            # use builtin 'replace' function here for convenience, can be changed to astroid transform
            assign_code = assign_code.replace(call_node.as_string(), target.as_string())
            new_body.append(astroid.extract_node(assign_code))
        else:
            new_body.append(node)
    block_node.body = new_body
    return block_node

def child_has_call(node):
    while hasattr(node, 'value'):
        if isinstance(node.value, astroid.nodes.Call):
            return True
        node = node.value
    return False


def get_call_node_from_subscript(node):
    while hasattr(node, 'value'):
        if isinstance(node.value, astroid.nodes.Call):
            return node.value
        node = node.value

def seems_like_custom_multilineblock_subscript(block_node):
    for node in block_node.body:
        if (isinstance(node, astroid.nodes.Return) or isinstance(node, astroid.nodes.Assign)) \
            and isinstance(node.value, astroid.nodes.Subscript) \
                and child_has_call(node):
            return True
    return False

def reformate(code):
    
    registerd_nodes = []
    for node_str in dir(astroid.nodes):
        try:
            if "__" in node_str or node_str=='IfExp' or node_str=='Lambda':
                continue
            node = astroid.nodes.__getattribute__(node_str)
            fields = node.__getattribute__(node, '_astroid_fields')
            if "body" in fields:
                astroid.MANAGER.register_transform(
                    node, 
                    multilineblock_list_comp_transform,
                    seems_like_custom_multilineblock_list_comp
                )
                astroid.MANAGER.register_transform(
                    node, 
                    multilineblock_subscript_transform,
                    seems_like_custom_multilineblock_subscript
                )
                registerd_nodes.append(node)
        except:
            pass
    # print("register node to transform:")
    # for node in registerd_nodes:
    #     print(node) 
    code = astroid.parse(code).as_string()
    for node in registerd_nodes:
        astroid.MANAGER.unregister_transform(
                node,
                multilineblock_list_comp_transform,
                seems_like_custom_multilineblock_list_comp
            )
        astroid.MANAGER.unregister_transform(
                node, 
                multilineblock_subscript_transform,
                seems_like_custom_multilineblock_subscript
            )
    
    # change all IfExp to If ... else ..., or we will not be able to perfrm correct rewrite
    astroid.MANAGER.register_transform(
        astroid.nodes.Assign,
        assign_transform,
        seems_like_custom_assign
    )
    astroid.MANAGER.register_transform(
        astroid.nodes.Return,
        return_transform,
        seems_like_custom_return
    )
    code = astroid.parse(code).as_string()
    astroid.MANAGER.unregister_transform(
        astroid.nodes.Assign,
        assign_transform,
        seems_like_custom_assign
    )
    astroid.MANAGER.unregister_transform(
        astroid.nodes.Return,
        return_transform,
        seems_like_custom_return
    )

    return code


def test():
    code = """
def a():
    user_uniform_avg_token_len = df_per_user.agg(sf.avg(df_per_user.token_count_avg)).collect()[0][0]
    b = [c for d in e]
    return [f if g else h for i in j]
"""
    print(reformate(code))