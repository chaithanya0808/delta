from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable
from datetime import datetime

# Initialize a Spark session
spark = SparkSession.builder.appName("DeltaTableCreation").getOrCreate()

# Set up Oracle connection properties
oracle_connection_properties = {
    "user": "your_username",
    "password": "your_password",
    "url": "jdbc:oracle:thin:@//your_oracle_host:your_port/your_service",
    "driver": "oracle.jdbc.driver.OracleDriver",
}

# Read data from Oracle table into a DataFrame
oracle_table_name = "your_oracle_table"
oracle_df = spark.read.jdbc(url=oracle_connection_properties["url"],
                            table=oracle_table_name,
                            properties=oracle_connection_properties)

# Add a processed status column with 'inprogress', a time column, and a stats column
delta_data = (
    oracle_df
    .withColumn("processed_status", lit("inprogress"))
    .withColumn("time", lit(datetime.now()))
    .withColumn("stats", lit("your_stats_value"))
)

# Write the DataFrame as a Delta table
delta_data.write.format("delta").mode("overwrite").save("/path/to/delta_table")

# Once you have created the Delta table, you can use DeltaTable for future operations
delta_table = DeltaTable.forPath(spark, "/path/to/delta_table")

# Example: Update processed status to 'processed' for a specific customer_id
customer_id_to_update = "customer1"
delta_table.update(
    condition=col("customer_id") == customer_id_to_update,
    set={"processed_status": lit("processed"), "time": lit(datetime.now())}
)
