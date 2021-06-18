# TaintStream

- A fine-grained (cell-level) taint tracking framework without runtime modification on Spark.
- Can be used for privacy compliance in big data platforms.

This repository contains the code and experiments for the paper:

> [ESEC/FSE 2021](https://2021.esec-fse.org/)
>
> [TaintStream: Fine-grained Taint Tracking for Big Data Platforms through Dynamic Code Translation]()

## Introduction

*TaintStream* is a cell-level taint tracking framework for big data platforms, e.g., Spark. TaintStream achieves taint tracking through two phases, i.e., **static code rewriting** and **dynamic code translation**.

In the static code rewriting phase, after taking a script as the input, *TaintStream* wraps the data processing piplines (e.g., select, filter, and groupBy) with a translation function Φ. The function will tranlate the code to properly maintain the taint tags of each data cell.

In the dynamic code translation phase, the original code will be translated to add the effect of taint propagation. Below is a simple example. 

```python
# given a dataframe with the following schema
# name | age 
# given a script processing this dataframe
df = df.filter("age" > 18)

# -----> code after static code rewriting
context = getContext()
code = "df.filter('age' > 18)"
df = phi(code, context) # translation function

# -----> code after dynamic code translation
# Note that during the execution, each data cell is attached with a taint tag.
# So the new schema of df_tag is
# |      name     |      age      |
# | value |  tag  | value |  tag  |
df_tag = df.filter(df_tag["age"]["value"] > 18)
```

See more details in the folder `lib` and also in our paper.

## Project Structure

```
root:
|-- benchmark: cell level benchmark, corresponding to the experiments in the paper
|-- example: an end-to-end example
|-- lib: code related to dynamic code translation
|-- scala: necessary utility functions wriiten in scala
|-- transformer: code related to static code rewriting
|-- taint_stream.py: entrance of TaintStream
|-- transform.py: transform the given script
```

For details, we leave a README file (if necessary) in each folder.

## Build and Test

Get TaintStream and run an end-to-end example.

```shell
git clone https://github.com/PKU-Chengxu/TaintStream.git
cd TaintStream
pip install -r requirements.txt
python taint_stream.py -wd ./example -i ExtractDocuments.py -t Author
```

Run paper experiments.

```
cd benchmark
python run.py
```

## Notes

- More detailed information is available in sub-folders.

- Please consider to cite our paper if you use the code or data in your research project.

> ```
> @inproceedings{yang2021taintstream,
>   title={TaintStream: Fine-grained Taint Tracking for Big Data Platforms through Dynamic Code Translation},
>   author={Yang, Chengxu and Li, Yuanchun and Xu, Mengwei and Chen, Zhenpeng and Liu, Yunxin and Huang, Gang and Liu, Xuanzhe},
>   booktitle={Proceedings of the 29th ACM Joint Meeting on European Software Engineering Conference and Symposium on the Foundations of Software Engineering},
>   year={2021}
> }
> ```
