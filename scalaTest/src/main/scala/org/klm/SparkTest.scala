package org.klm

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.sql.functions.{col,struct,lit}
import org.apache.spark.sql.types.{_}
object SparkTest extends App{

  //creating a spark Session
  val spark = SparkSession.builder()
    .appName("SparkTest")
    .master("local")
    .getOrCreate()

  import spark.implicits._
  //spark.sparkContext.setLogLevel("WARN")

  //schema of the input data
  val schema = new StructType().add(StructField("Year", StringType, true))
    .add(StructField("Industry_aggregation_NZSIOC", StringType, true))
    .add(StructField("Industry_code_NZSIOC", StringType, true))
    .add(StructField("Industry_name_NZSIOC", StringType, true))
    .add(StructField("Units", StringType, true))
    .add(StructField("Variable_code", StringType, true))
    .add(StructField("Variable_name", StringType, true))
    .add(StructField("Variable_category", StringType, true))
    .add(StructField("Value", StringType, true))
    .add(StructField("Industry_code_ANZSIC06", StringType, true))

  /*creating a streaming data reader object to read the  input csv
    csv* is required as readStream method only accepts a directory
   */
  val csvDf = spark.readStream.format("csv").option("header" , "true").schema(schema).csv("src/main/resources/survey.csv*")


  /*writing the stream to console ,
   console sink - output will be limited to default 20 records in the console mode.
   as there is no timestamp column in the dataset , "complete" mode is not possible
   */

  val df1 = csvDf.writeStream.format("console").outputMode("append")
    .start()

  df1.awaitTermination(10000)
  df1.stop()


  /*writing the stream to a parquet file ,
   limiting the output records 50
   as there is no timestamp column in the dataset , "complete" mode is not possible
   */


  val prqDf = csvDf.select(col("Variable_code"), col("Variable_name"))
    .writeStream.format("parquet").option("path" , "src/main/resources/output/survey") //.option("numRows",50)
    .option("checkpointLocation", "checkpoint").outputMode("append").start()

  prqDf.awaitTermination(10000)
  prqDf.stop()


  //creating a struct out of two columns and converting to Json
  val id_name_df = csvDf.select(struct(col("Variable_code"),col("Variable_name"))).as("value").toJSON

  //select partition key as 1
  val kafkaDf = id_name_df.selectExpr("CAST(1 AS STRING)", "CAST(value AS STRING)").as[(String, String)]

  //writing to kafka topic called first
  kafkaDf.writeStream.format("kafka").outputMode("append").option("kafka.bootstrap.servers","localhost:9092")
    .option("topic","survey2").option("checkpointLocation", "checkpoint").start().awaitTermination(10000)

}