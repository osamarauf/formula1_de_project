# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), True),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True),
])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

results_with_Column_df = add_ingestion_date(results_df).withColumnRenamed("resultId", "result_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("positionText", "position_text").withColumnRenamed("positionOrder", "position_order").withColumnRenamed("fastestLap", "fastest_lap").withColumnRenamed("fastestLapTime", "fastest_lap_time").withColumnRenamed("fastestLapSpeed", "fastest_lap_speed").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

results_final_df = results_with_Column_df.drop(col("statusId"))

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#   if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

results_final_df.write.mode("append").partitionBy("race_id").format("delta").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2

# COMMAND ----------

# overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_final_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1) FROM f1_processed.results GROUP BY 1

# COMMAND ----------

dbutils.notebook.exit("Success")
