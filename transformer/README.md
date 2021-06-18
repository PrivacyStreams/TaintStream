# Static Code Rewriting

This folder contains the code that corresponds to the static code rewriting phase of TaintSteam.

## transform.py

The entrance of the static code rewriting phase, which does the following things:

0. Perform user customized hooking. (refer to more detail in the dynamic code translation phase)
1. Code reformat to make the scripts more parsable by TaintStream. (refer to more details in `format_transformer.py`)
2. Parse the code and "pick out" the statements to be instrumented.
3. Instrument. (refer to more details in the instrument part)
4. Link dynamic code translation library to the original script.

## format_transform.py

Some of the python syntax is not friendly to the translation, e.g., list comprehension. So we first rewrite these statements to a normal assignment format. Below is an example.

```python
# original code
dfs = [df.select(cols[i]) for i in range(num_col)]

# code after reformat
dfs = []
for i in range(num_col):
  temp_ele = df.select(cols[i]) # this would be much more friendly
  dfs.append(temp_ele)
```

## Instrument

Briefly speaking, for each statement to be instrumented, we rewrite the code to do the following things:

1. collect necessary context;
2. wrap the statement with the translation function Φ.

Below is an example.

```python
# original code
df = df.select("name")

# code after rewriting
context = (globals().copy(), locals().copy()) # collect necessary context
code = 'df = df.select("name")' # original code
df = rewrite_and_execute(code, context) # wrap the code with phi()
```

