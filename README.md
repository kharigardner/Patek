# **Patek**

 A collection of reusable pyspark utility functions that help make development easier!

## Installation

Patek is available on PyPI and can be installed with pip:

```bash
pip install patek
```

## Usage

------------

### IO Helpers

Patek provides a set of IO helpers to quickly read and write data from/to various sources in PySpark.

#### *Dynamic Delta Table Writer*

The superDeltaWriter function allows you to write data to a Delta table using the merge capability without having to write out every single update and merge condition. This is useful when you have a large number of columns and/or a large number of update conditions.

```python

from patek.io import superDeltaWriter

superDeltaWriter(sparkDataframe, ['key_column1'], ['update_column1', 'update_column2'], 'delta_table_path', sparkSession)

```

If update columns are not specified, the default is to update all non-key columns that exist in both the source and target tables. Also, if the target table does not exist, it will be created. The default spark session passed is with the variable name 'spark'.
