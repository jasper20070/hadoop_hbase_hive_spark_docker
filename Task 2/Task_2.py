
"""Read the files from the HDFS and then write it into a Hive table"""

df1 = spark.read.csv("hdfs:///user/hadoop/drivers.csv", header=True, inferSchema=True)
df1.write.saveAsTable("drivers", format="hive", mode="overwrite")

df2 = spark.read.csv("hdfs:///user/hadoop/timesheet.csv", header=True, inferSchema=True)
df2.write.saveAsTable("timesheet", format="hive", mode="overwrite")

df3 = spark.read.csv("hdfs:///user/hadoop/truck_event_text_partition.csv", header=True, inferSchema=True)
df3.write.saveAsTable("truck_event_text_partition", format="hive", mode="overwrite")

spark.sql("SHOW TABLES").show()

"""Creating a Dataframe that contains the asked columns for aggregated information"""

df1 = spark.sql("SELECT driverId, name FROM drivers")
df2 = spark.sql("SELECT driverId, `hours-logged`, `miles-logged` FROM timesheet")

joined_df = df1.join(df2, on="DRIVERID", how="inner")

joined_df.show()