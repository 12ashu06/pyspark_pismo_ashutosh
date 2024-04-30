from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Pismo Assignment - Ashutosh") \
    .getOrCreate()

# Define the schema for the balance table
balance_schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("balance_order", IntegerType(), True),
    StructField("available_balance", FloatType(), True),
    StructField("status", StringType(), True)
])

# Create a list of dummy balance data
balance_data = [
    ("A1", 1, 1000.0, "ACTIVE"),
    ("A1", 2, 500.0, "ACTIVE"),
    ("B1", 1, 2000.0, "ACTIVE"),
    ("B1", 2, 1500.0, "ACTIVE"),
    ("C1", 1, 200.0, "ACTIVE"),
    ("C1", 2, 1000.0, "ACTIVE")
]

# Define the schema for the withdraw table
withdraw_schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("withdraw_amount", FloatType(), True),
    StructField("withdraw_order", IntegerType(), True)
])

# Create a list of dummy withdraw data
withdraw_data = [
    ("A1", 300.0, 1),
    ("B1", 2000.0, 1),
    ("C1", 1500.0, 2)
]

# Create a DataFrame from the dummy data and schema
balance_df = spark.createDataFrame(balance_data, schema=balance_schema)
withdraw_df = spark.createDataFrame(withdraw_data, schema=withdraw_schema)

# Convert column types if necessary
balance_df = balance_df.withColumn("balance_order", balance_df["balance_order"].cast("int"))
withdraw_df = withdraw_df.withColumn("withdraw_amount", withdraw_df["withdraw_amount"].cast("float"))

# Join balance and withdraw tables
joined_df = balance_df.join(
    withdraw_df,
    (balance_df["account_id"] == withdraw_df["account_id"]) & (balance_df["balance_order"] == withdraw_df["withdraw_order"]),
    "left"
)
joined_df = joined_df.fillna(0)
# joined_df.show()


# Sort joined DataFrame by balance order
joined_df = joined_df.orderBy("balance_order")

# Define a function to process withdrawals
def process_withdrawals(row):
    account_id = row["account_id"]
    balance_order = row["balance_order"]
    initial_balance = row["available_balance"]  # Store initial balance for validation
    withdraw_amount = row["withdraw_amount"]
    available_balance = row["available_balance"]
    
    # Check if withdrawal amount exceeds available balance
    if withdraw_amount > available_balance:
        validation_result = "Withdrawal amount exceeds available balance"
        return (account_id, balance_order, initial_balance, available_balance, "INSUFFICIENT FUNDS", validation_result)
    
    # Deduct withdrawal amount from available balance
    available_balance -= withdraw_amount
    
    # Check if balance becomes zero
    if available_balance == 0:
        status = "BALANCE WITHDREW"
    else:
        status = row["status"]  # Keep status unchanged
    
    # Update the row with new values
    return (account_id, balance_order, initial_balance, available_balance, status, "Withdrawal successful")