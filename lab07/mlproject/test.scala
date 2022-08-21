import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.{StringIndexerModel, IndexToString}
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}

object test extends App {
  val spark = SparkSession.builder().appName("lab07_test")
      .config("spark.master", "yarn")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  //Параметры запуска
  val topic_in = spark.sparkContext.getConf.get("spark.mlproject.in_topic")
  val topic_out = spark.sparkContext.getConf.get("spark.mlproject.model_dir")
  //val kafkaTopicOut = spark.sparkContext.getConf.get("spark.mlproject.out_topic")

  val train_dir = "lab07_train/train"
  val checkpoint_dir = "/user/ilya.chulyukin/checkpoint_lab07"

  //Модель
  val model = PipelineModel.load(train_dir)
  val indexer = model.stages(1).asInstanceOf[StringIndexerModel]
  val logreg = model.stages(2).asInstanceOf[LogisticRegressionModel]

  //Kafka
  //checkpoint
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  fs.delete(new Path(checkpoint_dir), true)
  //stream
  val stream_input = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("subscribe", topic_in)
    .load()
  //schema
  val visit_array = new StructType(
    Array(
      StructField("url", StringType),
      StructField("timestamp", LongType)
    )
  )
  val topic_in_Schema = new StructType(
    Array(
      StructField("uid", StringType),
      StructField("visits", ArrayType(visit_array))
    )
  )
  //read
  val in_stream = stream_input
    .select(
      from_json($"value".cast("string"), topic_in_Schema) as "value"
    )
    .select(
      $"value.*"
    )
  val dfInput = in_stream
    .select('uid, explode($"visits.url").as("src0"))
    .select('uid, regexp_replace(regexp_replace($"src0", "%3A%2F%2F","://"), "3A",":").as("src1"))
    .select('uid, regexp_replace($"src1", "((http)|(https))(://)","").as("src2"))
    .select('uid, regexp_replace($"src2", "^www.","").as("url_"))
    .select('uid, trim(split($"url_", "%2")(0)).as("url"))
    .select('uid, split($"url", "/")(0).as("domain")
    )
  val testing = dfInput.groupBy("uid").agg(collect_list("domain").as("domains"))

  //Модель
  //Исполнение модели
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
      $"gender_age"
    )
    .toJSON

  //Отправка
  val sink = out_.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("topic", topic_out)
    .option("checkpointLocation", checkpoint_dir)
    .outputMode("update")
  val sQuery = sink.start()
  sQuery.awaitTermination()

  spark.stop()

}
