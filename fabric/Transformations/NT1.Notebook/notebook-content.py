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
# META     "environment": {
# META       "environmentId": "9ede2c56-9171-8e7d-4726-7bd9930160d7",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import pyspark.sql.functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("users")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.agg({"*":"count"}).withColumnRenamed("count(1)", "count"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.filter(df["name"] == "timi").select("pk", "name"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.filter(F.col("name") == "timi").select(F.col("pk"), F.col("name")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.select("pk", "name").orderBy("pk", ascending=True))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.join(df, df["name"] == df["name"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.join(df, df["name"] == df["name"]).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read CSV
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ",") \
    .csv("Files/Data & Tech Case_Datasource 1.csv")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.toDF(*[col.replace(" ", "_") for col in df.columns])
df = df.toDF(*[col.replace("(", "") for col in df.columns])
df = df.toDF(*[col.replace(")", "") for col in df.columns])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write as Delta table
df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("LK1.staging.ipg")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("delta").load("abfss://WS1@onelake.dfs.fabric.microsoft.com/LK1.Lakehouse/Tables/staging/ipg")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dim_advertiser = (
    df
    .select(
        "Advertiser",
        "Insertion_Order",
        "Line_Item"
    )
    .distinct()
    .withColumn(
        "dimension_key",
        F.row_number().over(
            Window.orderBy(
                "Advertiser",
                "Insertion_Order",
                "Line_Item"
            )
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_advertiser.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("LK1.production.dim_advertiser")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%html
# MAGIC 
# MAGIC ### 3:22 in the video

# METADATA ********************

# META {
# META   "language": "html",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM LK1.staging.customers LIMIT 10
# MAGIC 
# MAGIC -- also %%pyspark, %%spark, %%sparkr, %%bash --> they change the whole cell
# MAGIC 
# MAGIC -- You can use just line comment with single %

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

param1 = "hello"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Notebookutils

notebookutils.gs.mkdirs()
notebookutils.fs.ls()
notebookutils.gs.rm()

notebookutils.notebook.run() # Run another notebook
notebookutils.notebook.exit()

notebookutils.credentials.getSecret()

notebookutils.lakehouse.list()
notebookutils.runtime.contextnotebookutils.session.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# From vscode

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
