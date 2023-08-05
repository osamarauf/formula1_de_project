# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

driver_df = spark.read.format("delta").load(f"{processed_folder_path}/driver").withColumnRenamed("number", "driver_number").withColumnRenamed("name", "driver_name").withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructor_df = spark.read.format("delta").load(f"{processed_folder_path}/constructor").withColumnRenamed("name", "team").withColumnRenamed("constructorId", "constructor_id")

# COMMAND ----------

circuit_df  = spark.read.format("delta").load(f"{processed_folder_path}/circuits").withColumnRenamed("location", "circuit_location")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races").withColumnRenamed("name", "race_name").withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

result_df = spark.read.format("delta").load(f"{processed_folder_path}/results").filter(f"file_date = '{v_file_date}'").withColumnRenamed("time", "race_time").withColumnRenamed("race_id", "result_race_id").withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

race_circuit_df = races_df.join(circuit_df, races_df.circuit_id == circuit_df.circuit_id).select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuit_df.circuit_location)

# COMMAND ----------

race_result_df = result_df.join(race_circuit_df, result_df.result_race_id == race_circuit_df.race_id).join(driver_df, result_df.driver_id == driver_df.driver_id).join(constructor_df, result_df.constructor_id == constructor_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_result_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position", "result_file_date").withColumnRenamed("result_file_date", "file_date") 

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------


