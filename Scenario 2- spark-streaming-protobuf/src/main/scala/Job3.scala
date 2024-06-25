import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.{Dataset, SparkSession, functions => F}
import scenario3.metricmessage.Metric2
import org.apache.spark.sql.streaming.Trigger

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

object Job3 {
  case class MyMetric(metricName: String, value: Double, timestamp: String, host: String, region: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark Streaming Job 2")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val kafkaBootstrapServers = "localhost:9092"
    val inputTopic = "protobuf-topic"

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .option("kafka.group.id", "unique-consumer-group-id")
      .load()

    val metrics = df
      .selectExpr("CAST(value AS STRING) as base64String")
      .map(row => {
        val base64String = row.getAs[String]("base64String")
        val decodedBytes = Base64.decodeBase64(base64String)
        val metric = Metric2.parseFrom(decodedBytes)
        MyMetric(metric.metricName, metric.value, metric.timestamp, metric.host, metric.region)
      })

    val metricNameColumn = "metricName"

    val query = metrics
      .writeStream
.foreachBatch { (batchDF: Dataset[MyMetric], batchId: Long) =>
  val metricNames = batchDF.select(metricNameColumn).distinct().as[String].collect()

  metricNames.foreach { metricName =>
    val filteredDF = batchDF.filter(F.col(metricNameColumn) === metricName)
    val tempDir = s"${metricName}_$batchId"

    filteredDF.coalesce(1)
      .write
      .mode("overwrite")
      .csv(tempDir)

    val tempFile = new File(tempDir).listFiles().find(_.getName.startsWith("part-")).get
    val finalFileName = s"${metricName}.csv"
    Files.move(tempFile.toPath, Paths.get(finalFileName), StandardCopyOption.REPLACE_EXISTING)

    new File(tempDir).listFiles().foreach(_.delete())
    new File(tempDir).delete()
  }
}
      .option("checkpointLocation", "/Users/omkarramakantnarayanpure/Downloads")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}
