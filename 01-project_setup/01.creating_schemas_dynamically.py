# Databricks notebook source
dbutils.widgets.text(
    name="env",
    defaultValue="",
    label="Enter the environment (dev, uat, prod)"
)

env = dbutils.widgets.get("env")

# COMMAND ----------

bronze_path = spark.sql(
    f"""DESCRIBE EXTERNAL LOCATION `bronze_{env}`"""
    ).select("url").collect()[0][0]

silver_path = spark.sql(
    f"""DESCRIBE EXTERNAL LOCATION `silver_{env}`"""
    ).select("url").collect()[0][0]

gold_path = spark.sql(
    f"""DESCRIBE EXTERNAL LOCATION `gold_{env}`"""
    ).select("url").collect()[0][0]


# COMMAND ----------

def create_Bronze_Schema(environment,path):
    
    print(f'Using {environment}_Catalog ')
    spark.sql(f"""USE CATALOG '{environment}_catalog'""")
    
    print(f'Creating Bronze Scheme in {environment}_Catalog')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `bronze` MANAGED LOCATION '{path}/bronze'""")

# COMMAND ----------

def create_Silver_Schema(environment,path):
    print(f'Using {environment}_Catalog')
    spark.sql(f""" USE CATALOG '{environment}_catalog'""")
    print(f'Creating Silver Schema in {environment}_Catalog')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `Silver` MANAGED LOCATION '{path}/silver'""")

# COMMAND ----------

def create_Gold_Schema(environment,path):
    print(f'Using {environment}_Catalog ')
    spark.sql(f""" USE CATALOG '{environment}_catalog'""")
    print(f'Creating Gold Schema in {environment}_Catalog')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `gold` MANAGED LOCATION '{path}/gold'""")

# COMMAND ----------

create_Bronze_Schema(env,bronze_path)
create_Silver_Schema(env,silver_path)
create_Gold_Schema(env,gold_path)