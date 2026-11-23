# Databricks notebook source
df_roads = spark.sql("select count(*) as road_count from dev_catalog.bronze.raw_roads")
print(df_roads.collect()[0]['road_count'])

df_traffic = spark.sql("select count(*) as traffic_count from dev_catalog.bronze.raw_traffic")
print(df_traffic.collect()[0]['traffic_count'])

# COMMAND ----------

df_roads = spark.sql("select count(*) as road_count from dev_catalog.silver.silver_roads")
print(df_roads.collect()[0]['road_count'])

df_traffic = spark.sql("select count(*) as traffic_count from dev_catalog.silver.silver_traffic")
print(df_traffic.collect()[0]['traffic_count'])

# COMMAND ----------

df_roads = spark.sql("select count(*) as road_count from dev_catalog.gold.gold_roads")
print(df_roads.collect()[0]['road_count'])

df_traffic = spark.sql("select count(*) as traffic_count from dev_catalog.gold.gold_traffic")
print(df_traffic.collect()[0]['traffic_count'])

df_roads = spark.sql("select count(*) as road_count from dev_catalog.gold.traffic_summary")
print(df_roads.collect()[0]['road_count'])

df_traffic = spark.sql("select count(*) as traffic_count from dev_catalog.gold.traffic_by_region")
print(df_traffic.collect()[0]['traffic_count'])

df_roads = spark.sql("select count(*) as road_count from dev_catalog.gold.road_usage_statistics")
print(df_roads.collect()[0]['road_count'])

df_traffic = spark.sql("select count(*) as traffic_count from dev_catalog.gold.ev_adoption_trend")
print(df_traffic.collect()[0]['traffic_count'])