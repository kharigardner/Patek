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

superDeltaWriter(sparkDataframe, ['key_column1'], 'delta/path', sparkSession, sparkContext, ['update_col1', 'update_col2'])

```

If update columns are not specified, the default is to update all non-key columns that exist in both the source and target tables. Also, if the target table does not exist, it will be created.

#### *Funnel.io Schema to Spark Schema*

The funnelSparkler function allows you to convert a Funnel.io schema to a Spark schema. This is useful to remove ambiguity when reading data from Funnel.io exports into spark dataframes, without having to manually define the schema.

```python
from patek.io import funnelSparkler

dataframe = funnelSparkler('path/to/funnel_schema.json', 'path/to/funnel_export_data', sparkSession, sparkContext, data_file_type='csv')
```

### Utility Functions

Patek provides a set of utility functions to help make development easier.

#### *Determine Key Candidates*

The determine_key_candidates function allows you to determine the key candidates for a given dataframe. This is useful when you have a large number of columns in a dataframe and you want to quickly determine which columns are good candidates for a key.

```python
from patek.utils import determine_key_candidates

key_candidates = determine_key_candidates(sparkDataframe)
print(key_candidates)

# Output:
# a list containing single column key candidates: ['column1', 'column2', 'column3']
# a list containing composite key candidates: [['column1', 'column2'], ['column1', 'column3']]
```

#### *Clean Column Names*

The column_cleaner function allows you to clean column names in a dataframe. It removes special characters and replaces spaces with underscores.

```python
from patek.utils import column_cleaner

# input dataframe columns: ['column?? 1', 'column: 2', 'column-3']

cleaned_dataframe = column_cleaner(sparkDataframe)

# output dataframe columns: ['column_1', 'column_2', 'column_3']
```
