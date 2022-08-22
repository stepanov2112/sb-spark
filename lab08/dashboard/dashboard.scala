import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.{StringIndexerModel, IndexToString}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.elasticsearch.spark.sql._


object dashboard extends App  {
  val spark = SparkSession.builder().appName("lab08_dashboard")
    .config("spark.master", "yarn")
    .config("spark.sql.session.timeZone", "UTC")
    .config("es.port","9200")
    .config("es.nodes", "10.0.0.31")
    .config("es.net.http.auth.user","timur_stepanov")
    .config("es.net.http.auth.pass","loQaulNj")
    .config("spark.es.nodes.wan.only", "true")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._


val input_dir = "hdfs:///labs/laba08/laba08.json"

val train_dir = "lab07_train/train"
val checkpoint_dir = "/user/timur_stepanov/checkpoint_lab07"

val model = PipelineModel.load(train_dir)
val indexer = model.stages(1).asInstanceOf[StringIndexerModel]
val logreg = model.stages(2).asInstanceOf[LogisticRegressionModel]


  val dfInput = spark.read.json(input_dir)
    .select('uid, to_date(from_unixtime(substring(col("date"),1,10)),"yyyy-MM-dd").as("date"), explode($"visits.url").as("src0"))
    .select('uid, 'date, regexp_replace(regexp_replace($"src0", "%3A%2F%2F","://"), "3A",":").as("src1"))
    .select('uid, 'date, regexp_replace($"src1", "((http)|(https))(://)","").as("src2"))
    .select('uid, 'date, regexp_replace($"src2", "^www.","").as("url_"))
    .select('uid, 'date, trim(split($"url_", "%2")(0)).as("url"))
    .select('uid, 'date, split($"url", "/")(0).as("domain")
    )

  val testing = dfInput.groupBy("uid","date").agg(collect_list("domain").as("domains"))

val predicts = model.transform(testing)
//Получение результатов
val receive = new IndexToString()
  .setInputCol(logreg.getPredictionCol)
  .setOutputCol(indexer.getInputCol)
  .setLabels(indexer.labels)

val out_ = receive
    .transform(predicts)
    .select(
      $"uid",
      $"gender_age",
      $"date"
    )

val out_ = receive
    .transform(predicts)
    .select(
      $"uid",
      $"gender_age",
      $"date"
    )

out_.saveToEs("timur_stepanov_lab08/_doc")

out_.saveToEs("timur_stepanov_lab08/_doc")

  val elasticIndex = "timur_stepanov_lab08"

  val source_es = spark.sqlContext.read
    .format("org.elasticsearch.spark.sql")
    /*     .option("es.net.proxy.socks.host", "localhost")
         .option("es.net.proxy.socks.port", "1080") */
    .option("es.port","9200")
    .option("es.nodes", "10.0.0.31")
    .option("es.net.http.auth.user","timur.stepanov")
    .option("es.net.http.auth.pass","loQaulNj")
   val dfEs = source_es.load(elasticIndex)

dfEs.show()


spark.stop()
}