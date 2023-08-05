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

from pyspark.sql.types import StructField, StructType, IntegerType, StringType 

# COMMAND ----------

constructor_schema = StructType(fields=[StructField("constructorId", IntegerType(), False),
                                        StructField("constructorRef", StringType(), True),
                                        StructField("name", StringType(), True), 
                                        StructField("nationality", StringType(), True),
                                        StructField("url", StringType(), True) 
])

# COMMAND ----------

constructor_df = spark.read.schema(constructor_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(constructor_df["url"])

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_dropped_df).withColumnRenamed("contructorId", "contructor_id").withColumnRenamed("contructorRef", "contructor_ref").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructor")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructor

# COMMAND ----------

dbutils.notebook.exit("Success")
