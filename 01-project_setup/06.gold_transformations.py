# Databricks notebook source
dbutils.widgets.text(
    name="env",
    defaultValue="",
    label="Enter the environment (dev, uat, prod)"
)

env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %run "./commons"

# COMMAND ----------

# ---- Reading data from Silver_Traffic table ----

def read_SilverTrafficTable(environment):
    print('Reading the Silver Traffic Table Data : ',end='')
    df_SilverTraffic = (spark.readStream.table(f"`{environment}_catalog`.`silver`.silver_traffic"))
    print(f'Reading {environment}_catalog.silver.silver_traffic Success!')
    print("**********************************")
    return df_SilverTraffic

# COMMAND ----------

# ---- Reading data from silver_roads table -----

def read_SilverRoadsTable(environment):
    print('Reading the Silver Table Silver_roads Data : ',end='')
    df_SilverRoads = (spark.readStream.table(f"`{environment}_catalog`.`silver`.silver_roads"))
    print(f'Reading {environment}_catalog.silver.silver_roads Success!')
    print("**********************************")
    return df_SilverRoads

# COMMAND ----------

# ---- Creating vehicle Intensity Column ----

def create_VehicleIntensity(df):
    from pyspark.sql.functions import col, try_divide
    print('Creating Vehicle Intensity column : ',end='')
    df_veh = df.withColumn('Vehicle_Intensity',
                try_divide (
                    col('Motor_Vehicles_Count'),
                    col('Link_length_km')
                )
    )
    print("Success!!!")
    print('***************')
    return df_veh

# COMMAND ----------

# ---- Creating LoadTime column ----

def create_LoadTime(df):
    from pyspark.sql.functions import current_timestamp
    print('Creating Load Time column : ',end='')
    df_timestamp = df.withColumn('Load_Time', current_timestamp())
    print('Success!!')
    print('**************')
    return df_timestamp

# COMMAND ----------

# ---- Writing Data to Gold Traffic ----

def write_Traffic_GoldTable(StreamingDF,environment):
    print('Writing the gold_traffic Data : ',end='') 

    write_gold_traffic = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "/GoldTrafficLoad/Checkpt/")
                .outputMode('append')
                .queryName("GoldTrafficWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}_catalog`.`gold`.`gold_traffic`"))
    
    write_gold_traffic.awaitTermination()
    print(f'Writing `{environment}_catalog`.`gold`.`gold_traffic` Success!')

# COMMAND ----------

# ---- Writing Data to Gold Roads ----

def write_Roads_GoldTable(StreamingDF,environment):
    print('Writing the gold_roads Data : ',end='') 

    write_gold_roads = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "/GoldRoadsLoad/Checkpt/")
                .outputMode('append')
                .queryName("GoldRoadsWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}_catalog`.`gold`.`gold_roads`"))
    
    write_gold_roads.awaitTermination()
    print(f'Writing `{environment}_catalog`.`gold`.`gold_roads` Success!')

# COMMAND ----------

# ---- Calling all functions ----

## Reading from Silver tables
df_SilverTraffic = read_SilverTrafficTable(env)
df_SilverRoads = read_SilverRoadsTable(env)
    
## Tranformations     
df_vehicle = create_VehicleIntensity(df_SilverTraffic)
df_FinalTraffic = create_LoadTime(df_vehicle)
df_FinalRoads = create_LoadTime(df_SilverRoads)

## Writing to gold tables    
write_Traffic_GoldTable(df_FinalTraffic,env)
write_Roads_GoldTable(df_FinalRoads,env)

# COMMAND ----------

# ---- Traffic Volume year wise ----

def summarize_traffic_volume(df):
    from pyspark.sql.functions import sum as _sum, col, year, month
    df_traffic_summary = (df
                            .groupBy("year")
                            .agg(
                                _sum(
                                    col("cars_and_taxis")
                                    + col("buses_and_coaches")
                                    + col("lgv_type")
                                    + col("hgv_type")
                                    + col("two_wheeled_motor_vehicles")
                                    + col("pedal_cycles")
                                    + col("ev_car")
                                    + col("ev_bike")
                                ).alias("total_vehicles"),
                                _sum("cars_and_taxis").alias("total_cars"),
                                _sum("ev_car").alias("total_ev_cars")
                            )
                        )
    return df_traffic_summary

# COMMAND ----------

def write_traffic_summary_GoldTable(StreamingDF,environment):
    print('Writing the gold_traffic_summary Data : ',end='') 

    write_gold_traffic = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "/GoldTraffic_summary/Checkpt/")
                .outputMode('complete')
                .queryName("GoldTrafficWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}_catalog`.`gold`.`traffic_summary`"))
    
    write_gold_traffic.awaitTermination()
    print(f'Writing `{environment}_catalog`.`gold`.`traffic_summary` Success!')

# COMMAND ----------

# ---- Traffic by Region / Local Authority ----

def summarize_traffic_by_region(df):
    from pyspark.sql.functions import sum as _sum, col

    df_region_summary = (df
                            .groupBy("region_id", "region_name", "local_authority_name")
                            .agg(
                                _sum("cars_and_taxis").alias("cars"),
                                _sum("lgv_type").alias("lgv"),
                                _sum("hgv_type").alias("hgv"),
                                _sum("ev_car").alias("ev_cars"),
                                _sum("pedal_cycles").alias("cycles"),
                                _sum("buses_and_coaches").alias("buses"),
                                _sum(
                                    col("cars_and_taxis") 
                                    + col("lgv_type") 
                                    + col("hgv_type") 
                                    + col("ev_car") 
                                    + col("buses_and_coaches") 
                                    + col("pedal_cycles") 
                                    + col("two_wheeled_motor_vehicles") 
                                    + col("ev_bike")
                                ).alias("total_traffic")
                            )
                        )
    return df_region_summary

# COMMAND ----------

def write_traffic_region_GoldTable(StreamingDF, environment):
    print('Writing the gold_traffic_by_region Data : ', end='') 

    write_gold_region = (
        StreamingDF.writeStream
        .format('delta')
        .option('checkpointLocation', checkpoint + "/GoldTraffic_region/Checkpt/")
        .outputMode('complete')
        .queryName("GoldTrafficRegionWriteStream")
        .trigger(availableNow=True)
        .toTable(f"`{environment}_catalog`.`gold`.`traffic_by_region`")
    )
    
    write_gold_region.awaitTermination()
    print(f'Writing `{environment}_catalog`.`gold`.`traffic_by_region` Success!')

# COMMAND ----------

 # ---- Road Usage Statistics ----

 def summarize_road_usage(df):
    from pyspark.sql.functions import sum as _sum, col

    df_road_summary = (
        df
        .groupBy("road_name", "road_category_id")
        .agg(
            _sum(
                col("cars_and_taxis") 
                + col("lgv_type") 
                + col("hgv_type") 
                + col("buses_and_coaches")
                + col("two_wheeled_motor_vehicles")
                + col("ev_car") 
                + col("ev_bike")
            ).alias("total_motor_vehicles"),
            _sum("pedal_cycles").alias("total_cycles"),
            _sum(
                col("cars_and_taxis") 
                + col("lgv_type") 
                + col("hgv_type") 
                + col("buses_and_coaches")
                + col("two_wheeled_motor_vehicles")
                + col("ev_car") 
                + col("ev_bike")
                + col("pedal_cycles")
            ).alias("total_traffic")
        )
    )
    return df_road_summary

# COMMAND ----------

def write_road_usage_GoldTable(StreamingDF, environment):
    print('Writing the gold_road_usage Data : ', end='') 

    write_gold_road = (
        StreamingDF.writeStream
        .format('delta')
        .option('checkpointLocation', checkpoint + "/GoldRoad_usage/Checkpt/")
        .outputMode('complete')
        .queryName("GoldRoadUsageWriteStream")
        .trigger(availableNow=True)
        .toTable(f"`{environment}_catalog`.`gold`.`road_usage_statistics`")
    )
    
    write_gold_road.awaitTermination()
    print(f'Writing `{environment}_catalog`.`gold`.`road_usage_statistics` Success!')

# COMMAND ----------

# ---- EV Adoption Trend ----

def summarize_ev_adoption(df):
    from pyspark.sql.functions import sum as _sum, col

    df_ev_summary = (
        df
        .groupBy("year")
        .agg(
            _sum("ev_car").alias("total_ev_cars"),
            _sum("ev_bike").alias("total_ev_bikes"),
            _sum(col("ev_car") + col("ev_bike")).alias("total_ev")
        )
        .orderBy("year")
    )
    return df_ev_summary

# COMMAND ----------

def write_ev_adoption_GoldTable(StreamingDF, environment):
    print('Writing the gold_ev_adoption Data : ', end='') 

    write_gold_ev = (
        StreamingDF.writeStream
        .format('delta')
        .option('checkpointLocation', checkpoint + "/GoldEV_adoption/Checkpt/")
        .outputMode('complete')
        .queryName("GoldEVAdoptionWriteStream")
        .trigger(availableNow=True)
        .toTable(f"`{environment}_catalog`.`gold`.`ev_adoption_trend`")
    )
    
    write_gold_ev.awaitTermination()
    print(f'Writing `{environment}_catalog`.`gold`.`ev_adoption_trend` Success!')

# COMMAND ----------

# ---- Calling functions ----

## Reading from Silver tables
df_SilverTraffic = read_SilverTrafficTable(env)
df_SilverRoads = read_SilverRoadsTable(env)

# summarize_traffic_volume
df_traffic_summary = summarize_traffic_volume(df_SilverTraffic)
write_traffic_summary_GoldTable(df_traffic_summary,env)

# summarize_traffic_by_region
df_region_summary = summarize_traffic_by_region(df_SilverTraffic)
write_traffic_region_GoldTable(df_region_summary, env)

# summarize_road_usage
df_road_summary = summarize_road_usage(df_SilverTraffic)
write_road_usage_GoldTable(df_road_summary, env)

# summarize_ev_adoption
df_ev_summary = summarize_ev_adoption(df_SilverTraffic)
write_ev_adoption_GoldTable(df_ev_summary, env)