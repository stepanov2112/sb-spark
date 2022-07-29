import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_unixtime, split, substring}

object filter extends App {

  val spark = SparkSession.builder().appName("lab04").config("spark.master", "local[*]").getOrCreate()
  import spark.implicits._

  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  val odp = spark.conf.get("spark.filter.output_dir_prefix")
  val topic = spark.conf.get("spark.filter.topic_name")
  val offset = spark.conf.get("spark.filter.offset")
  val offset_prefix = """{""""
  val offset_middle = """" : {"0" : """
  val offset_postfix = "}}"

  val offset_ = if (offset == "earliest" ) {offset} else {s"""{"$topic":{"0":$offset}}"""}

  println (offset_)
  fs.delete(new Path(odp), true)

  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" ->  topic,
    "startingOffsets" -> offset_
  )

  val df = spark.read.format("kafka").options(kafkaParams).load
  val jsonString = df.select('value.cast("string")).as[String]
  val parsed = spark.read.json(jsonString)

  val DF2 = parsed.withColumn("date",from_unixtime(substring(col("timestamp"),1,10),"YYYYMMDD"))
  DF2.cache()

  val DF2_buy = DF2.filter('event_type === "buy")
  val DF2_view = DF2.filter('event_type === "view")

  DF2_buy.write.mode("Overwrite").partitionBy("date").format("json").save(odp.concat("/buy"))
  DF2_view.write.mode("Overwrite").partitionBy("date").format("json").save(odp.concat("/view"))

  spark.stop()
}