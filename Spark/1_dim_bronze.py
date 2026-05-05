# Databricks notebook source
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DateType, TimestampType,
FloatType)
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = 'ecommerce'

#Define schema for the data file
brand_schema = StructType([
    StructField("brand_code",  StringType(), False),
    StructField("brand_name",  StringType(), True),
    StructField("catalog_code", StringType(), True),

])


# COMMAND ----------

raw_data_path = "/Volumes/ecommerce/source_data/raw/brands/*.csv"

# COMMAND ----------

df = spark.read.option('header', "true").option("delimiter", ",").schema(brand_schema).csv(raw_data_path)

df = df.withColumn("_source_file", F.col("_metadata.file_path")) \
    .withColumn("ingested_at", F.current_timestamp())
display(df.limit(5))

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catagory

# COMMAND ----------

category_schema = StructType([
    StructField("category_code", StringType(), False),
    StructField("category_name", StringType(), True)
])

# Load data using the schema defined
raw_data_path = "/Volumes/ecommerce/source_data/raw/category/*.csv"


df_raw = spark.read.option('header', "true").option("delimiter", ",").schema(category_schema).csv(raw_data_path)

df_raw = df_raw.withColumn("_ingested_at", F.current_timestamp()) \
               .withColumn("_source_file", F.col("_metadata.file_path"))

# write raw data to the Bronze layer (catalog: ecommerce. Schema: Bronze, table: brz_category)

df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product

# COMMAND ----------

product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand_code", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("material", StringType(), True),
    StructField("weight_grams", StringType(), True),
    StructField("length_cm", StringType(), True),
    StructField("width_cm", FloatType(), True),
    StructField("height_cm", FloatType(), True),
    StructField("rating_count", IntegerType(), True)
])

# Load data using the schema defined
raw_data_path = "/Volumes/ecommerce/source_data/raw/products/*.csv"

df = spark.read.option("header", "true").option("delimiter", ",").schema(product_schema).csv(raw_data_path) \
    .withColumn("file_name", F.col("_metadata.file_path")) \
    .withColumn("ingest_timestamp", F.current_timestamp())

# Write raw data to the Bronze layer (catalog:ecommerce, schema: bronze, table: brz_products)

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True)
])

# Load Data using the schema defined
raw_data_path = "/Volumes/ecommerce/source_data/raw/customers/*.csv"

df_raw = spark.read.option("header", "true").option("delimiter", ",").schema(customers_schema).csv(raw_data_path) \
    .withColumn("file_name", F.col("_metadata.file_path")) \
    .withColumn("ingest_timestamp", F.current_timestamp()) \
    .drop("email")

# Write raw data to the Bronze Layer (catelog: ecommerce, schema: bronze, table: brz_customers )

df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_customers")

# COMMAND ----------

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True)
])

# Load Data using the schema defined
raw_data_path = "/Volumes/ecommerce/source_data/raw/customers/*.csv"

df_raw = spark.read.option("header", "true").option("delimiter", ",").schema(customers_schema).csv(raw_data_path) \
    .withColumn("file_name", F.col("_metadata.file_path")) \
    .withColumn("ingest_timestamp", F.current_timestamp()) \
    .drop("email")

# Write raw data to the Bronze Layer (catelog: ecommerce, schema: bronze, table: brz_customers )

df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date

# COMMAND ----------

# Define schema for the file

date_schema = StructType([
    StructField("date", StringType(), True),
    StructField("year", StringType(), True),
    StructField("day_name", StringType(), True),
    StructField("quarter", StringType(), True),
    StructField("week_of_year", IntegerType(), True),
])

# Load data using the schema defined 
raw_data_path = "/Volumes/ecommerce/source_data/raw/date/*.csv"

df_raw = spark.read.option("header", "true").option("delimiter", ",").schema(date_schema).csv(raw_data_path) 

df_raw = df_raw.withColumn("ingest_timestamp", F.current_timestamp()) \
               .withColumn("_source_file", F.col("_metadata.file_path"))

# Write raw data to the Bronze Layer (catelog: ecommerce, schema: bronze, table: brz_customers )

df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_calender")


# COMMAND ----------

