"""
This module contains helper functions for working with reading and writing data with Apache Spark.
"""

import delta
from pyspark.sql.utils import AnalysisException, ParseException
from pyspark.sql.column import Column as SparkColumn
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from .exceptions import InvalidArgumentError
from pyspark.sql import types
import ujson

def superDeltaWriter(
    dataframe: SparkDataFrame,
    key_cols: list,
    path,
    spark: SparkSession,
    sparkcontext: SparkContext,
    update_cols: list = None,
):
    # lets initialize logging first for this function
    logger4J = sparkcontext._jvm.org.apache.log4j
    logger = logger4J.LogManager.getLogger(__name__)
    try:
        print("checking if the delta table exists")
        # checking if the delta table exists
        delta_table = delta.tables.DeltaTable.forPath(spark, path)
    except AnalysisException as e:
        # if it doesn't exist, create it
        dataframe.write.format("delta").mode("append").save(path)
        print(
            f"Created delta table at {path}, as there was no delta table at the path specified."
        )
    else:
        if not (
            merge_statements := [
                f"target.{col} = source.{col}"
                for col in key_cols
                if col in dataframe.columns and col in delta_table.toDF().columns
            ]
        ):
            raise InvalidArgumentError(
                "Key columns must be present in both the dataframe and the delta table."
            )
        merge_statement = " and ".join(merge_statements)
        # log the merge statement
        logger.info(f"Merge statement for table at {path} : {merge_statement}")
        if update_cols:
            update_keys = {
                col: f"source.{col}"
                for col in update_cols
                if col in dataframe.columns and col in delta_table.toDF().columns
            }
        else:
            update_keys = {
                col: f"source.{col}"
                for col in dataframe.columns
                if col not in key_cols and col in delta_table.toDF().columns
            }
        # log the update keys
        logger.info(f"Update keys for table at {path} : {update_keys}")
        delta_table.alias("target").merge(
            dataframe.alias("source"), merge_statement
        ).whenMatchedUpdate(set=update_keys).whenNotMatchedInsertAll().execute()
        print(f"Updated delta table at {path}.")
    finally:
        print("Finished writing to delta table.")


# Map JSON schema types to Spark types
TYPE_MAPPING = {
    "str": types.StringType(),
    "int": types.IntegerType(),
    "dec": types.FloatType(),
    "boolean": types.BooleanType(),
    "timestamp": types.TimestampType(),
    "date": types.DateType()
}


def funnelSparkler(json_schema_path: str, csv_path: str) -> SparkDataFrame:
    # Create a Spark session
    spark = SparkSession.builder.getOrCreate()

    # Read the JSON schema file from the Hadoop file system
    json_schema_df = spark.read.json(json_schema_path)

    # Convert the JSON schema types to Spark types
    schema = [
        types.StructField(row["name"], TYPE_MAPPING[row["type"]], nullable=False)
        for row in json_schema_df.collect()
    ]

    # Use the schema to load the CSV file
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(types.StructType(schema))
        .load(csv_path)
    )

    # Return the resulting DataFrame
    return df