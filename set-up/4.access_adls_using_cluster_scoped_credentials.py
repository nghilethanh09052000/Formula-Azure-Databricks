# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Date Lake Using Cluster Scoped Credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlnghi.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlnghi.dfs.core.windows.net/circuits.csv"))