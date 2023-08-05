# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuit_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("location", StringType(), True),
                                    StructField("country", StringType(), True),
                                    StructField("lat", DoubleType(), True),
                                    StructField("lng", DoubleType(), True),
                                    StructField("alt", DoubleType(), True),
                                    StructField("url", StringType(), True),
])

# COMMAND ----------

circuit_df = spark.read.schema(circuit_schema).csv(f"{raw_folder_path}/{v_file_date}/circuits.csv", header=True)

# COMMAND ----------

circuit_selected_column = circuit_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuit_selected_column = circuit_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuit_renamed_df = circuit_selected_column.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref").withColumnRenamed("lat","latitude").withColumnRenamed("lng", "logitude").withColumnRenamed("alt", "altitude").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

circuit_final_df = add_ingestion_date(circuit_renamed_df)

# COMMAND ----------

circuit_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")
