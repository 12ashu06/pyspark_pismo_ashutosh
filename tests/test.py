import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from common import process_withdrawals

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("pytest") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_process_withdrawals(spark_session):
    # Define the schema for the balance table
    balance_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("balance_order", IntegerType(), True),
        StructField("available_balance", FloatType(), True),
        StructField("status", StringType(), True)
    ])

    # Define the schema for the withdraw table
    withdraw_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("withdraw_amount", FloatType(), True),
        StructField("withdraw_order", IntegerType(), True)
    ])

    # Create a DataFrame from the dummy data and schema
    balance_data = [("A1", 1, 1000.0, "ACTIVE")]
    withdraw_data = [("A1", 300.0, 1)]
    balance_df = spark_session.createDataFrame(balance_data, schema=balance_schema)
    withdraw_df = spark_session.createDataFrame(withdraw_data, schema=withdraw_schema)

    # Apply the process_withdrawals function to the row
    row = balance_df.join(withdraw_df, balance_df["account_id"] == withdraw_df["account_id"], "inner").first()
    result = process_withdrawals(row)
    print(result)
    # Assert the result
    assert result == ("A1", 1, 1000.0, 700.0, "ACTIVE", "Withdrawal successful")

def test_process_withdrawals_sufficient_funds(spark_session):
    # Test case for withdrawal with sufficient funds
    balance_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("balance_order", IntegerType(), True),
        StructField("available_balance", FloatType(), True),
        StructField("status", StringType(), True)
    ])

    withdraw_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("withdraw_amount", FloatType(), True),
        StructField("withdraw_order", IntegerType(), True)
    ])

    balance_data = [("A1", 1, 1000.0, "ACTIVE")]
    withdraw_data = [("A1", 300.0, 1)]
    balance_df = spark_session.createDataFrame(balance_data, schema=balance_schema)
    withdraw_df = spark_session.createDataFrame(withdraw_data, schema=withdraw_schema)

    row = balance_df.join(withdraw_df, balance_df["account_id"] == withdraw_df["account_id"], "inner").first()
    result = process_withdrawals(row)

    assert result == ("A1", 1, 1000.0, 700.0, "ACTIVE", "Withdrawal successful")

def test_process_withdrawals_zero_balance(spark_session):
    # Test case for withdrawal resulting in zero balance
    balance_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("balance_order", IntegerType(), True),
        StructField("available_balance", FloatType(), True),
        StructField("status", StringType(), True)
    ])

    withdraw_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("withdraw_amount", FloatType(), True),
        StructField("withdraw_order", IntegerType(), True)
    ])

    balance_data = [("A1", 1, 1000.0, "ACTIVE")]
    withdraw_data = [("A1", 1000.0, 1)]
    balance_df = spark_session.createDataFrame(balance_data, schema=balance_schema)
    withdraw_df = spark_session.createDataFrame(withdraw_data, schema=withdraw_schema)

    row = balance_df.join(withdraw_df, balance_df["account_id"] == withdraw_df["account_id"], "inner").first()
    result = process_withdrawals(row)

    assert result == ("A1", 1, 1000.0, 0.0, "BALANCE WITHDREW", "Withdrawal successful")