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

# MAGIC %run "./commons"  

# COMMAND ----------

# ---- Reading from bronze table ----

def read_BronzeTrafficTable(environment):
    print('Reading the Bronze Table Data : ',end='')
    df_bronzeTraffic = (spark.readStream.table(f"`{environment}_catalog`.`bronze`.raw_traffic"))
    print(f'Reading {environment}_catalog.bronze.raw_traffic Success!')
    return df_bronzeTraffic

# COMMAND ----------

# ---- Creating a new column with total Electric vehicles count ----

def ev_Count(df):
    print('Creating Electric Vehicles Count Column : ', end='')
    from pyspark.sql.functions import col
    df_ev = df.withColumn('Electric_Vehicles_Count', col('EV_Car') + col('EV_Bike'))
    
    print('Success!! ')
    return df_ev

# COMMAND ----------

# ---- Creating a new column with total vehicles count ----

def Motor_Count(df):
    print('Creating All Motor Vehicles Count Column : ', end='')
    from pyspark.sql.functions import col
    df_motor = df.withColumn('Motor_Vehicles_Count',
                            col('Electric_Vehicles_Count') + col('Two_wheeled_motor_vehicles') + col('Cars_and_taxis') + col('Buses_and_coaches') + col('LGV_Type') + col('HGV_Type')
                            )
    
    print('Success!! ')
    return df_motor

# COMMAND ----------

# ---- Adding a timestamp column showing when the data was transformed -----

def create_TransformedTime(df):
    from pyspark.sql.functions import current_timestamp
    print('Creating Transformed Time column : ',end='')
    df_timestamp = df.withColumn('Transformed_Time', current_timestamp())
    print('Success!!')
    return df_timestamp

# COMMAND ----------

# ---- Writing the Transformed data to Silver_Traffic Table ----

def write_Traffic_SilverTable(StreamingDF,environment):
    print('Writing the silver_traffic Data : ',end='') 

    write_StreamSilver = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "/SilverTrafficLoad/Checkpt/")
                .outputMode('append')
                .queryName("SilverTrafficWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}_catalog`.`silver`.`silver_traffic`"))
    
    write_StreamSilver.awaitTermination()
    print(f'Writing `{environment}_catalog`.`silver`.`silver_traffic` Success!')

# COMMAND ----------

# Calling functions for: all 5 transforamation + writing data to silver table

## Reading the bronze traffic data
df_trafficdata = read_BronzeTrafficTable(env)

#  To remove duplicate rows
df_dups = remove_Dups(df_trafficdata)

# To raplce any NULL values
Allcolumns =df_dups.schema.names
df_nulls = handle_NULLs(df_dups,Allcolumns)

## To get the total EV_Count
df_ev = ev_Count(df_nulls)

## To get the Total Motor vehicle count
df_motor = Motor_Count(df_ev)

## Calling Transformed time function
df_final = create_TransformedTime(df_motor)

## Writing to silver_traffic
write_Traffic_SilverTable(df_final, env)

# COMMAND ----------

display(spark.sql(f"SELECT Count(*) FROM `{env}_catalog`.`silver`.`silver_traffic` LIMIT 10"))