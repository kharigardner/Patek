from pyspark.sql.utils import AnalysisException, ParseException
from pyspark.sql.column import Column as SparkColumn
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from .exceptions import InvalidArgumentError
from pyspark.sql import types
import itertools
from pyspark.rdd import RDD
from pyspark.sql.types import StructType

def determine_key_candidates(dataframe: SparkDataFrame) -> list or str:
    """
    This function will return a list of key candidates for a dataframe. It accounts for single column keys, as well as composite keys. It will iterate through every column and row to get every possible combination of unique values.
    """
    # get the list of columns
    columns = dataframe.columns

    # init the list of key candidates
    key_candidates = []

    # this first for loop will determine single column keys and add them to the list of key candidates
    for col in columns:
        # count the number of unique values in the column
        unique_values = dataframe.select(col).distinct().count()
        # if the number of unique values is equal to the number of rows, then the column is a key
        if unique_values == dataframe.count():
            key_candidates.append(col)
            print(f"Found single column key: {col}")

    # this next for loop will determine composite keys and add them to the list of key candidates
    # first, lets get the list of all possible combinations of columns
    column_combo_list = []
    for i in range(1, len(columns)+1):
        column_combo_list.extend(list(itertools.combinations(columns, i)))
    # now, lets iterate through the list of column combinations
    for combo in column_combo_list:
        # count the number of unique values in the combination of columns
        unique_values = dataframe.select(combo).distinct().count()
        # if the number of unique values is equal to the number of rows, then the combination of columns is a key
        if unique_values == dataframe.count():
            key_candidates.append(combo)
            print(f"Found composite key: {combo}")

    # return the list of key candidates
    return key_candidates

def column_cleaner(dataframe: SparkDataFrame) -> SparkDataFrame:
    """
    This function cleans the column names of a dataframe by replacing spaces with underscores and removing special characters.
    """
    dataframe = dataframe.toDF(*[c.replace(' ', '_').replace('.', '_').replace('-', '_').replace('(', '').replace(')', '').replace('%', '').replace(':', '') for c in dataframe.columns])
    return dataframe

def _extracted_from_rearrange_columns_11(rdd, schema, spark: SparkContext):
    # use a map function to rearrange the columns of the rdd in the order of the schema
    rdd_rearranged = rdd.map(lambda x: [x[i] for i in schema.fieldNames() if i in x])
    # assert that the columns are in the correct order
    for i in range(len(schema)):
        try:
            assert rdd_rearranged.first()[i] == schema[i].name
        except AssertionError as e:
            raise AssertionError(f"Column {schema[i].name} is not in the correct position. Unable to rearrange columns.") from e
    return spark.createDataFrame(rdd_rearranged, schema)

# defining a function to rearrange the columns of a rdd to match the order of a schema, to prevent data in the wrong columns when reading in a csv
def rearrange_columns(schema: StructType, spark: SparkContext, rdd: RDD = None, path: str = None,) -> SparkDataFrame:
    """
    This function rearranges the columns of a rdd to match the order of a schema, to prevent data in the wrong columns when reading in a csv.
    """
    if rdd is None and path is None:
        raise InvalidArgumentError("You must pass either an RDD or a path to a csv file.")
    elif rdd is not None and path is not None:
        raise InvalidArgumentError("You must pass either an RDD or a path to a csv file, not both.")
    elif rdd:
        return _extracted_from_rearrange_columns_11(rdd, schema, spark)
    elif path:
        # read in the csv file as an rdd
        rdd = spark.textFile(path)
        return _extracted_from_rearrange_columns_11(rdd, schema, spark)