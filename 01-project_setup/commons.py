# Databricks notebook source
# MAGIC %md
# MAGIC ### To re-use common functions and variables

# COMMAND ----------

dbutils.widgets.text(
    name="env",
    defaultValue="",
    label="Enter the environment (dev, uat, prod)"
)

env = dbutils.widgets.get("env")

# COMMAND ----------

# ---- Defining all common variables ----

checkpoint = spark.sql(f"describe external location `checkpoints_{env}`").select("url").collect()[0][0]
landing = spark.sql(f"describe external location `landing_{env}`").select("url").collect()[0][0]
bronze = spark.sql(f"describe external location `bronze_{env}`").select("url").collect()[0][0]
silver = spark.sql(f"describe external location `silver_{env}`").select("url").collect()[0][0]
gold = spark.sql(f"describe external location `gold_{env}`").select("url").collect()[0][0]

print(f"checkpoint: {checkpoint}")
print(f"landing: {landing}")
print(f"bronze: {bronze}")
print(f"silver: {silver}")
print(f"gold: {gold}")

# COMMAND ----------

# ---- Removing duplicates ----

def remove_Dups(df):
    print('Removing Duplicate values: ',end='')
    df_dup = df.dropDuplicates()
    print('Success!')
    return df_dup

# COMMAND ----------

# ---- Handling NULLs ----

def handle_NULLs(df,Columns):
    print('Replacing NULLs of Strings DataType with "Unknown": ', end='')
    df_string = df.fillna('Unknown',subset=Columns)
    print('Success!')
    print('Replacing NULLs of Numeric DataType with "0":  ', end='')
    df_numeric = df_string.fillna(0,subset=Columns)
    print('Success!')

    return df_numeric