import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}

object Job4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Average Aggregation Job")
      .master("local[*]")
      .getOrCreate()

    val schema = new StructType()
      .add("metricName", StringType, nullable = true)
      .add("value", DoubleType, nullable = true)
      .add("timestamp", StringType, nullable = true)
      .add("host", StringType, nullable = true)
      .add("region", StringType, nullable = true)

    val inputDir = "/Users/onkarramakantnarayanpure/Documents/spark-streaming-protobuf/cpu_usage_percentage.csv"
    val outputDir = "/Users/onkarramakantnarayanpure/Documents/spark-streaming-protobuf/average_aggregation.csv"

    val df = spark.read.schema(schema).csv(inputDir)

    val avgDF = df.groupBy("metricName").agg(avg("value").as("avg_value"))

    avgDF.write.mode("overwrite").csv(outputDir)

    spark.stop()
  }
}
