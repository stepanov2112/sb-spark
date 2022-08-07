import org.apache.spark.sql.streaming.Trigger

import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions.{col, from_unixtime, substring, from_json, explode}

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.types._

import org.apache.spark.sql._

 

import org.apache.spark.sql.expressions.Window

import org.apache.spark.sql.functions._

 


object agg extends App {

 

 

  val spark = SparkSession.builder().appName("lab04").config("spark.master", "local[*]").getOrCreate()

  import spark.implicits._

 

  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

 

 

  val topic = "timur_stepanov"

 

  def createKafkaSinkWithCheckpoint(chkName: String, df: DataFrame) = {

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

      .writeStream

      .outputMode("update")

      .format("kafka")

      .option("kafka.bootstrap.servers", "spark-master-1:6667")

      .option("topic", "yuriy_severyukhin_lab04b_out")

      .trigger(Trigger.ProcessingTime("5 seconds"))

      .option("checkpointLocation", s"chk/$chkName")

      .option("truncate", "true")

  }

 

 

  /* val offset_ = if (offset == "earliest" ) {offset} else

   {s"""{"$topic":{"0":$offset}}"""} */

 

  val offset_ = "latest"

 

  println (offset_)

 

  val schema = StructType(List(

    StructField("event_type", StringType, true)

    ,StructField("category", StringType, true)

    ,StructField("item_id", StringType, true)

    ,StructField("item_price", IntegerType, true)

    ,StructField("uid", StringType, true)

    ,StructField("timestamp", StringType, true)

  ))

 
  val kafkaParams = Map(

    "kafka.bootstrap.servers" -> "spark-master-1:6667",

    "subscribe" ->  topic,

    "startingOffsets" -> offset_ ,

    "maxOffsetsPerTrigger" -> "1000"

  )

 
  val sdf = spark.readStream.format("kafka").options(kafkaParams).load

 
  val parsedSdf = sdf.select('value.cast("string"), 'topic, 'partition, 'offset)

 
  val parsed = sdf.select('value.cast("string"))


  val parsed_ = parsed.select(

    from_json(col("value").cast("string"), schema).alias("value_"))


  val parsed__ = parsed_

    .withColumn("event_type",col("value_").getItem("event_type"))

    .withColumn("category",col("value_").getItem("category"))

    .withColumn("item_id",col("value_").getItem("item_id"))

    .withColumn("item_price",col("value_").getItem("item_price"))

    .withColumn("uid",col("value_").getItem("uid"))

    .withColumn("timestamp",col("value_").getItem("timestamp"))

    .withColumn("date",from_unixtime(substring(col("timestamp"),1,10)))

 

    .drop("values_")

 

  val grouped =

 

    parsed__

      .withColumn("vis_c", when(length(col("uid")) > 2 /*&& col("event_type") === "view"*/, 1).otherwise(0))

      .withColumn("price_b", when(col("event_type") === "buy" , 'item_price).otherwise(null))

 

 

      .groupBy(window($"date", "60 minutes"))

      .agg(

        sum('vis_c).as("visitors"),

        sum('price_b).as("revenue"),

        count('price_b).as("purchases")

      )

      .withColumn("aov", when(col("purchases") === "0" , 0).otherwise(col("revenue")/col("purchases")))

      .withColumn("start_ts",unix_timestamp(col("window").getItem("start")))

      .withColumn("end_ts",unix_timestamp(col("window").getItem("end")))

      .drop("window")

 

 

  val structed = grouped.select('start_ts as "key", struct('start_ts, 'end_ts,'revenue,'visitors,'purchases,'aov).alias("s"))

  val json_ds = structed.withColumn("value", to_json('s)).drop('s)

 

  /* val grouped = parsed__.groupBy("event_type").count() */

  val chkstr = "chk01"

 

  fs.delete(new Path(s"chk"), true)

 

 

  val sink = createKafkaSinkWithCheckpoint(chkstr,json_ds)

 

  sink.start()
}
