import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions._
 
import org.apache.spark.sql.expressions.Window
 
object features extends App {
 
  val spark = SparkSession.builder().appName("lab05").config("spark.master", "local[*]").getOrCreate()
 
  import spark.implicits._
 
  val parqDF = spark.read.parquet("/labs/laba03/weblogs.parquet")
 
  val parqDF_ = parqDF.select(parqDF("*"), explode(parqDF("visits"))
    .alias("visits_")).drop("visits")
 
  val parqDF__ = parqDF_
    .withColumn("url", col("visits_").getItem("url"))
    .withColumn("timestamp", col("visits_").getItem("timestamp"))
    .drop(col("visits_"))
 
  val parqDF___ = parqDF__
    .withColumn("domain_", callUDF("parse_url", col("url"), lit("HOST")))
    .drop(col("url"))
    .withColumn("domain", regexp_replace(col("domain_"), "www.", ""))
    .drop(col("domain_"))
 
  val parqDF_dw = parqDF___
    .withColumn("dw", concat(lit("web_day_"), lower(date_format(from_unixtime(substring(col("timestamp"), 1, 10)), "EE"))))
    .groupBy("uid").pivot("dw").count
 
  val parqDF_hour = parqDF___
    .withColumn("h", hour(to_utc_timestamp(from_unixtime(substring(col("timestamp"), 1, 10)),"Europe/Moscow")))
    .withColumn("w", when(col("h") < 18 && col("h") >= 9, lit(1)).otherwise(0))
    .withColumn("e", when(col("h") >= 18, lit(1)).otherwise(0))
 
  val parqDF_hour_p = parqDF_hour.withColumn("hh", concat(lit("web_hour_"), 'h))
    .groupBy("uid").pivot("hh").count().na.fill(0)
 
  val parqDF_hour_d = parqDF_hour.
    groupBy("uid")
    .agg(
      sum('w).as("w_sum"),
      sum('e).as("e_sum"),
      count('uid).as("c")
    )
    .withColumn("web_fraction_work_hours", 'w_sum / 'c)
    .withColumn("web_fraction_evening_hours", 'e_sum / 'c)
    .drop('w_sum)
    .drop('e_sum)
    .drop('c)
 
  val parqDF_g = parqDF___.groupBy("domain").count()
 
  val windowSpec = Window.orderBy('count.desc)
  val parqDF_ranged = parqDF_g.filter('domain > ".").withColumn("rn", row_number() over (windowSpec)).filter(expr("rn <= 1000"))
 
  val parqDF_alph = parqDF_ranged.withColumn("domain__", regexp_replace($"domain", "\\.", ""))
  val parqDF_1000 = parqDF___.join(parqDF_alph, Seq("domain"), "inner")
  val parqDF_1000_c = parqDF_1000.groupBy("uid", "domain").count().orderBy("domain")
  val parqDF_1000_uid = parqDF_1000_c.select('uid).distinct()
  val parqDF_1000_all = parqDF_1000_uid.crossJoin(parqDF_alph).select("uid", "domain")
 
  val parqDF_1000_full = parqDF_1000_all.join(parqDF_1000_c, Seq("uid", "domain"), "left").na.fill(0).orderBy("uid", "domain")
  val parqDF_1000_c_ = parqDF_1000_full.groupBy("uid").
    agg(collect_list("count").alias("id_list"))
 
  val parqDF_1000_c2 = parqDF_1000_c_.withColumn("domain_features", 'id_list).drop('id_list)
 
  val df_old = spark.read.parquet("users-items/20200429")
 
  val df_sss = df_old.join(parqDF_1000_c2, Seq("uid"), "full")
    .join(parqDF_dw, Seq("uid"), "full")
    .join(parqDF_hour_d, Seq("uid"), "full")
    .join(parqDF_hour_p, Seq("uid"), "full")
    .na.fill(0)
 
  df_sss.write.mode("Overwrite").format("parquet").save("features")}