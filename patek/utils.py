import delta
from pyspark.sql.utils import AnalysisException, ParseException
from pyspark.sql.column import Column as SparkColumn
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from .exceptions import InvalidArgumentError
from pyspark.sql import types
import itertools

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