from pyspark.sql import Row, SparkSession
import pandas as pd
from datetime import datetime

from ..cleaning_utils import *

def test_lowercase_all_columns():
    # ASSEMBLE
    test_data = [
        {
            "ID": 1,
            "First_Name": "Bob",
            "Last_Name": "Builder",
            "Age": 24
        },
        {
            "ID": 2,
            "First_Name": "Sam",
            "Last_Name": "Smith",
            "Age": 41
        }
    ]

    spark = SparkSession.builder.getOrCreate()
    test_df = spark.createDataFrame(map(lambda x: Row(**x), test_data))

    # ACT 
    output_df = lowercase_all_column_names(test_df)

    output_df_as_pd = output_df.toPandas()

    expected_output_df = pd.DataFrame({
        "id": [1, 2],
        "first_name": ["Bob", "Sam"],
        "last_name": ["Builder", "Smith"],
        "age": [24, 41]
    })
    # ASSERT
    pd.testing.assert_frame_equal(left=expected_output_df,right=output_df_as_pd, check_exact=True)

def test_uppercase_all_columns():
    # ASSEMBLE
    test_data = [
        {
            "ID": 1,
            "First_Name": "Bob",
            "Last_Name": "Builder",
            "Age": 24
        },
        {
            "ID": 2,
            "First_Name": "Sam",
            "Last_Name": "Smith",
            "Age": 41
        }
    ]

    spark = SparkSession.builder.getOrCreate()
    test_df = spark.createDataFrame(map(lambda x: Row(**x), test_data))
    
    # ACT 
    output_df = uppercase_all_column_names(test_df)

    output_df_as_pd = output_df.toPandas()

    expected_output_df = pd.DataFrame({
        "ID": [1, 2],
        "FIRST_NAME": ["Bob", "Sam"],
        "LAST_NAME": ["Builder", "Smith"],
        "AGE": [24, 41]
    })
    # ASSERT 
    pd.testing.assert_frame_equal(left=expected_output_df,right=output_df_as_pd, check_exact=True)


def test_add_metadata():
    # ASSEMBLE 
    test_data = [
        {
            "id": 1,
            "first_name": "Bob",
            "last_name": "Builder",
            "age": 24
        },
        {
            "id": 2,
            "first_name": "Sam",
            "last_name": "Smith",
            "age": 41
        }
    ]

    now = datetime.now()
    field_dict = {
        "task_id": 1,
        "ingested_at": now
    }
    spark = SparkSession.builder.getOrCreate()
    test_df = spark.createDataFrame(map(lambda x: Row(**x), test_data))

    # ACT 
    output_df = add_metadata(df=test_df, field_dict=field_dict)

    output_df_as_pd = output_df.toPandas()

    expected_output_df = pd.DataFrame({
        "id": [1, 2],
        "first_name": ["Bob", "Sam"],
        "last_name": ["Builder", "Smith"],
        "age": [24, 41],
        "task_id": [1, 1],
        "ingested_at": [now, now]
    })
    # ASSERT 
    pd.testing.assert_frame_equal(left=expected_output_df,right=output_df_as_pd, check_exact=True, check_dtype=False)