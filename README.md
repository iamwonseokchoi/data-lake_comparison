
# Comparison Notebook (Work In Progress)

## Overview

Work in Progress

This mini study performs various comparison operations, possibly involving big data processing using PySpark and Scala Spark. Comparison of Delta Lake and Iceberg datalake frameworks are included, with a focus on usage methods for both as well as performance when handling high and low cardinality data transformations and collations. 

## Findings

In short, it seems computationally efficient to keep data in raw Parquet, Avro, ORC formats until the need for ACID compliance and data lineage and history management is required. Only then should the process of deciding which data lake framework to use be inserted into the equation. 

Simply in terms of performance, Iceberg seems to offer more ease-of-use compared to Delta Lake, although Delta Lake is more flexible when it comes to working in a big data context and processing data using Apache Spark. Although Pyiceberg can be used for Iceberg tables, Delta does seem to offer a more robust approach to a Pythonic API via the Delta-Spark library. 

In terms of partitioning, there is little performant difference between Delta and Iceberg, but in terms of bucketing and ease of manipulating on-the-fly for schemas and storage methods, Iceberg is easier to modify ad-hoc due to the architecture of how Iceberg stores data. Bucketing is more performant with Iceberg as Delta offers no direct method to bucket data. For high cardinality data warehousing, Iceberg could be more flexible. 

There is no "one size fits all" which is expected and each datalake framework has its own benefits. 

Data used for comparison can be generated within the script. 

## Requirements

- Python 3.x
- PySpark
- Delta Lake

### Python Libraries

The following Python libraries are required:

```
pyspark
delta
```

Additionally, a custom module `spark_config` is used for creating Spark sessions.

### System Requirements

- Spark cluster or local Spark setup
- Sufficient memory and computing resources depending on the data size

## How to Run

1. Ensure that all the dependencies are installed.
2. Clone this repository or download the `comparison.ipynb` notebook.
3. Open the notebook using Jupyter Notebook or Jupyter Lab.
4. Run the cells in sequence from top to bottom.

## Structure

The notebook contains a total of 60 cells, which include:

- 31 Markdown cells for documentation and explanation
- 29 Code cells containing the core logic

## Features

- Data import and export
- Data transformation and analysis using PySpark and Delta Lake
- Various comparison operations

## Custom Modules

The notebook uses a custom module named `spark_config` for Spark session creation. Make sure to have this module available in your working directory or Python path.
