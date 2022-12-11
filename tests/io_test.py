import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
from patek.io import superDeltaWriter
from patek.exceptions import InvalidArgumentError


class TestSuperDeltaWriter(unittest.TestCase):
    # Set up a mock Spark context and dataframe
    @patch("patek.spark", MagicMock(spec=SparkSession))
    @patch("patek.sc", MagicMock())
    def setUp(self):
        self.dataframe = MagicMock()
        self.key_cols = ["col1", "col2"]
        self.path = "path/to/delta/table"

    # Test that the function creates the delta table if it doesn't exist
    def test_create_table(self):
        with patch("patek.delta") as mock_delta:
            mock_delta.tables.DeltaTable.forPath.side_effect = AnalysisException
            superDeltaWriter(self.dataframe, self.key_cols, self.path)
            self.dataframe.write.assert_called_with(
                "delta", mode="append", save=self.path
            )
            self.assertEqual(mock_delta.tables.DeltaTable.forPath.call_count, 1)

    # Test that the function updates the delta table if it exists
    def test_update_table(self):
        with patch("patek.delta") as mock_delta:
            delta_table = MagicMock()
            mock_delta.tables.DeltaTable.forPath.return_value = delta_table
            delta_table.toDF.return_value.columns = self.key_cols + ["col3"]
            self.dataframe.columns = self.key_cols + ["col3"]
            superDeltaWriter(self.dataframe, self.key_cols, self.path)
            delta_table.alias.assert_called_with("target")
            delta_table.alias().merge.assert_called()
            self.assertEqual(mock_delta.tables.DeltaTable.forPath.call_count, 1)

    # Test that the function raises an error if the key columns are not present in both the dataframe and the delta table
    def test_missing_key_columns(self):
        with patch("patek.delta") as mock_delta:
            delta_table = MagicMock()
            mock_delta.tables.DeltaTable.forPath.return_value = delta_table
            delta_table.toDF.return_value.columns = ["col1", "col3"]
            self.dataframe.columns = self.key_cols
            with self.assertRaises(InvalidArgumentError):
                superDeltaWriter(self.dataframe, self.key_cols, self.path)

    def test_update_cols_argument(self):
        with patch("patek.delta") as mock_delta:
            delta_table = MagicMock()
            mock_delta.tables.DeltaTable.forPath.return_value = delta_table
            delta_table.toDF.return_value.columns = self.key_cols + ["col3"]
            self.dataframe.columns = self.key_cols + ["col3"]
            update_cols = ["col3"]
            superDeltaWriter(
                self.dataframe, self.key_cols, self.path, update_cols=update_cols
            )
            delta_table.alias.assert_called_with("target")
            delta_table.alias().merge.assert_called()
            delta_table.alias().merge().whenMatchedUpdate.assert_called_with(
                set={"col3": "source.col3"}
            )
            self.assertEqual(mock_delta.tables.DeltaTable.forPath.call_count, 1)

if __name__ == '__main__':
    unittest.main()