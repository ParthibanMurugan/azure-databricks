// Databricks notebook source
val data = Seq((1, "parthiban", "murugan", 34), (2, "preethi", "m",24))
import spark.implicits._

val rdd = spark.sparkContext.parallelize(data)

val df = rdd.toDF("id","fName","lName","age")

df.write.mode("append").format("parquet").save("/data/presonal")


// COMMAND ----------

val newDF = spark.read.format("parquet").load("/data/presonal")
display(newDF)

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.time.LocalDate 
import java.time.format.DateTimeFormatter

val schema = StructType.fromDDL("id INT,firstName STRING,middleName STRING,lastName STRING,gender STRING,birthDate STRING,ssn STRING,salary DOUBLE")
val dataDF = spark.read.format("csv")
.options(Map("header"->"true"))
.schema(schema)
.load("/Volumes/demo_databricks/default/test/export.csv")

dataDF.show(false)
dataDF.printSchema

// COMMAND ----------

import io.delta.tables.DeltaTable

DeltaTable.createIfNotExists(spark)
.tableName("demo_databricks.default.peoples")
.addColumn("id","INT")
.addColumn("firstName","STRING")
.addColumn("middleName","STRING")
.addColumn("lastName","STRING")
.addColumn("gender","STRING")
.addColumn("birthDate","STRING")
.addColumn("ssn","STRING")
.addColumn("salary","DOUBLE")
.execute()

// COMMAND ----------

val data = DeltaTable.forName(spark,"demo_databricks.default.peoples")

data.as("demo")
.merge(
  dataDF.as("demo1"),"demo1.id = demo.id"
)
.whenMatched
.updateAll
.whenNotMatched
.insertAll
.execute

