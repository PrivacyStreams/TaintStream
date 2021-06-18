# Dynamic Code Translation

This folder `lib` contains the code that corresponds to the dynamic code translation phase.

## Folder Structure

```
lib root
|-- dependency: code related to data flow graph, which if for the implicit flow handling
|-- external: user customized hooking, refer to more details in hooking part.
|-- phi: core of the dynamic code translation.
|-- udf: pre-defined utility function, e.g., tagAgg.
|-- taint_dataframe.py: code that related to taint a dataframe.
|-- taint_stream_lib.jar: A jar that contains the lib written in scala.
```

Note that all the folder will be copy to the work dir of a script by TaintStream as a library.

## Data Flow Graph

TaintStream will generate a column-level data flow graph for each script after the execution. The graph shows the column-level dependency between columns after each operator. The graph is output in a json format with the name `dependency_graph`. We leave more details in our paper.

## User Customized Hooking

Following the spark API, we can taint the dataframe only in a column level (implemented in `taint_dataframe.py`). To achieve the cell-level tainting, we implement a hooking mechanism. Users could provide their own tagged input data and statement and replace the original input statement by specifying the configuration file though a `-c` argument. 

Below is an example of the configuration file:

```json
{
    "insert_code": "from lib.external.basic import *\n",
    "customized_hook": {
        "emails = spark.read.json(input_dir, schema=schema)": 
      					"emails = tagged_email_reader()",
        "numbers = spark.read.json(input_dir, schema=schema)": 
      					"numbers = tagged_number_reader()"
    }
}
```

`"insert_code"` will be added to the head of the script, in this example, we add an import statement to include the our own input function.

`customized_hook` is a map where the key is the original code and the value is the code to replace with. Currently, the hooking is implemented through string replacement, so be care with the redundant or lost blank space.

We have implemented the necessary hookings (located in the external forder) for the scripts in our benchmark for reference.

## Phi

phi (Φ) is the core function that translates a statement to properly propagate taint tags. Here we give a brief description to each file and leave more details in the paper.

|       File       | Description                                                  |
| :--------------: | ------------------------------------------------------------ |
|   constant.py    | Include some constant value, e.g., type of the tag.          |
|      phi.py      | Entrance of phi function. It acts as a router, which will parse the code and route to the corresponding handler. E.g., route the code using SQL APIs to phi_stmt and route the map/reduce APIs to phi_mapreduce. |
| phi_mapreduce.py | Handler of the code using map/reduce APIs.                   |
|   phi_stmt.py    | Handler of the code using SQL APIs. It also acts as a router, which will parse the code and route to the corresponding operators' (like select, groupBy) handler. |
|    phi_col.py    | Handler of the column. More details in the paper (Sec 2.1, Sec 3.3). |
|   phi_cond.py    | Handler of the condition. E.g.,  In the paper, we merge condition with the col. We implement separately for performance concerns. |
|     pudf.py      | Include some predefined pandas udf. (Deprecated, will be removed in the future) |
|     util.py      | Include some helper functions for dynamic translation.       |

## Scala Code and Pre-defined UDF

To improve the performance, we implement the following functions (`tagMerge, tagAgg, getTag, getVal, pack `) in scala instead of python.

The code in `scala` folder shows how we implement the aforementioned functions. The code in `udf` folder shows how we invoke them in python (through py4j).

Note that for a new type of taint tag, corresponding modification is required for the aforementioned functions. We've implemented Boolean, Integer, String types in the `scala` folder for reference.

More information is available [here](https://stackoverflow.com/questions/41780141/how-to-use-scala-udf-in-pyspark). 

Also feel free to contact me: yangchengxu@pku.edu.cn.