# CellBench

- A cell level benchmark for taint tracking in spark.

This folder contains the code and experiments for the evaluation on the accuracy of TaintStream.

## Run the benchmark

```
cd benchmark
python run.py
```

The script do the following things, for each category:

1. Generate the ground truth.
2. Generate random input data.
3. Test original data processing scripts.
4. Test TaintStream on the scripts in step 4.
5. Evaluate and report the results.

## Categories

Currently, we include the following categories of scripts:

|    Category     | Description                                                  |
| :-------------: | :----------------------------------------------------------- |
|      Basic      | Scripts that contains basic operators (APIs) like select, orderBy, filter... |
|     GroupBy     | Scripts that contains groupBy operators and aggregation functions. |
|      Join       | Scripts that contains join operators.                        |
|    MapReduce    | Scripts that contains Map/reduce operators.                  |
| Structured Data | Scripts whose input/output data is structured, e.g., array type, nested structure. |
|       UDF       | Scripts that contains user defined functions.                |

Each category is organized in a separate folder, with the following structure:

```
category root
|-- ground_truth_generator.py: generate ground truth for this category
|-- XX_data_generator.py: generate input data and tagged input data for the scripts
|-- {category}_config.json: config file that indicates TaintStreams about the hooking
|-- other python files: data processing scripts.
```