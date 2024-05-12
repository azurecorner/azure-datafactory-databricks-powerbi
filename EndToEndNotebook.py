# Databricks notebook source
dbutils.fs.mount(
   source = "wasbs://raw@datasyncstdata.blob.core.windows.net",
   mount_point = "/mnt/datasynchro",
   extra_configs = {"fs.azure.account.key.datasyncstdata.blob.core.windows.net":"ygdiJ6wnJjvb6NB8veg9EgJWz0UjahlzhodZpzg+9+qBMF2dT7V3rkL2d1++n0yRN7VNSYw4uGWP+AStsIyTZQ=="}
)


# COMMAND ----------

dbutils.fs.ls("/mnt/datasynchro/")

# COMMAND ----------

df = spark.read.format("csv").options(header='True',inferSchema='True').load('dbfs:/mnt/datasynchro/dbo.pizza_sales.txt')

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("pizza_sales_analysis")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  pizza_sales_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   order_id,
# MAGIC   quantity,
# MAGIC   date_format(order_date,"MMM" )  month_name,
# MAGIC   date_format(order_date,"EEEE" )  day_name,
# MAGIC   hour(order_time) order_time,
# MAGIC   unit_price,
# MAGIC   total_price,
# MAGIC   pizza_size,
# MAGIC   pizza_category,
# MAGIC   pizza_name
# MAGIC from  pizza_sales_analysis 

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   count( distinct order_id) order_id,
# MAGIC  sum(quantity) quantity,
# MAGIC   date_format(order_date,"MMM" )  month_name,
# MAGIC   date_format(order_date,"EEEE" )  day_name,
# MAGIC   hour(order_time) order_time,
# MAGIC  sum(unit_price) unit_price,
# MAGIC  sum(total_price) total_sales,
# MAGIC   pizza_size,
# MAGIC   pizza_category,
# MAGIC   pizza_name
# MAGIC from  pizza_sales_analysis
# MAGIC group by 3,4,5,8,9,10
