# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Date Lake Using Access Key
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file
# MAGIC

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope="formula1-scope", key="formula1dl-account-key")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlnghi.dfs.core.windows.net",
    "j3FlNXSdSQ/KuYpeN6nmeCq3FtWMy8TnhbphZ64I60kFUNBp6QQyPaj5DRkSVFRtBPupIsLJuKAH+AStK0PnIQ=="
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlnghi.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlnghi.dfs.core.windows.net/circuits.csv"))