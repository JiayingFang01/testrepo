# Databricks notebook source
# MAGIC %md #### Workshop for ETL

# COMMAND ----------

df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv', header = True)
# we add header here, because if we don't set it, it will automatically add _c1, _c2 ... as the column names

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_driver =  spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header = True)
df_driver.count()

# COMMAND ----------

display(df_driver)

# COMMAND ----------

# MAGIC %md #### Transform data

# COMMAND ----------

from pyspark.sql.functions import datediff, current_date, avg
from pyspark.sql.types import IntegerType

# COMMAND ----------

df_driver = df_driver.withColumn('age', datediff(current_date(), df_driver.dob))
# now in the age column, should transfer the days to the years

# COMMAND ----------

df_driver = df_driver.withColumn('age', df_driver['age'].cast(IntegerType()))

# COMMAND ----------

display(df_driver)

# COMMAND ----------

df_lap_drivers = df_driver.select('driverID', 'age', 'forename', 'nationality', 'surname', 'url').join(df_laptimes, on=['driverID'])

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md #### Aggregate by Age

# COMMAND ----------

df_lap_drivers = df_lap_drivers.groupBy('nationality', 'age').agg(avg('milliseconds'))

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md #### Store Data in S3

# COMMAND ----------

df_lap_drivers.write.csv('s3://jf3583-gr5069/processed/in_class_workshop/laptimes_by_drivers.csv')

# COMMAND ----------


