# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ff3b968d-8c21-4645-9e47-6366b4449df7",
# META       "default_lakehouse_name": "LK1",
# META       "default_lakehouse_workspace_id": "a53675cc-050d-4846-abbc-c5e719e82670",
# META       "known_lakehouses": [
# META         {
# META           "id": "ff3b968d-8c21-4645-9e47-6366b4449df7"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     },
# META     "mirrored_db": {
# META       "known_mirrored_dbs": []
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import os
import json
import pyspark.sql.functions as F
import time

source_path = "abfss://WS1@onelake.dfs.fabric.microsoft.com/LK1.Lakehouse/Files/streaming"
checkpoint_path = "abfss://WS1@onelake.dfs.fabric.microsoft.com/LK1.Lakehouse/Files/streaming/checkpoint"
schema_name = "streaming"
table_name = "temperature_stream"

# Schema for incoming JSON data
file_schema = StructType() \
    .add("id", StringType()) \
    .add("temperature", DoubleType()) \
    .add("timestamp", TimestampType())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Read streaming data to unbounded table/dataframe
raw_stream_df = spark.readStream \
    .schema(file_schema) \
    .option("maxFilesPerTrigger", 1) \
    .json(source_path)

# Example transformation that adds a processed_timestamp column to the data
transformed_stream_df = raw_stream_df \
    .withColumn("processed_timestamp", \
    F.current_timestamp())

# Stream data to a delta table
deltastream = transformed_stream_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .start(f"Tables/{schema_name}/{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.isActive

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.status

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
