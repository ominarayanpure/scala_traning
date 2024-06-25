import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, DefaultCredentialsProvider}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object BroadcastExample extends App {

  // Set log level to reduce Spark logs in the console
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Get AWS credentials
  val credentialsProvider: AwsCredentialsProvider = DefaultCredentialsProvider.builder.build
  val awsCredentials = credentialsProvider.resolveCredentials()

  // Create Spark session with necessary configurations
  def createSparkSession(): SparkSession = {
    SparkSession.builder
      .appName("Broadcast Example")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1") // Set the bind address
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.access.key", awsCredentials.accessKeyId())
      .config("spark.hadoop.fs.s3a.secret.key", awsCredentials.secretAccessKey())
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
      .getOrCreate()
  }

  // Initialize the Spark session
  val spark: SparkSession = createSparkSession()

  import spark.implicits._

  // Create sample dataset 1
  val data1 = Seq(
    ("Zaragoza CAMS station 0", "2020-08-01 01:00:00", 9.235579),
    ("Zaragoza CAMS station 1", "2020-08-01 02:00:00", 11.345678),
    ("Zaragoza CAMS station 2", "2020-08-01 03:00:00", 10.456789)
  ).toDF("station_name", "Date", "NO2")

  // Create sample dataset 2
  val data2 = Seq(
    ("Zaragoza CAMS station 0", "Location1"),
    ("Zaragoza CAMS station 1", "Location2"),
    ("Zaragoza CAMS station 2", "Location3")
  ).toDF("station_name", "location")

  // Read the previous CSV file from S3
  val csvFile = "s3a://scala-training-bucket/demmo1/zaragoza_data.csv"
  val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvFile)

  val renamedDF = df
    .withColumnRenamed("Wind-Speed (U)", "Wind_Speed_U")
    .withColumnRenamed("Wind-Speed (V)", "Wind_Speed_V")
    .withColumnRenamed("Dewpoint Temp", "Dewpoint_Temp")
    .withColumnRenamed("Total Percipitation", "Total_Precipitation")
    .withColumnRenamed("Relative Humidity", "Relative_Humidity")

  // Task 1: Join without Broadcasting
  val startTimeWithoutBroadcast = System.nanoTime()

  val joinedDFWithoutBroadcast = renamedDF.join(data2, "station_name")
  val resultWithoutBroadcast = joinedDFWithoutBroadcast.groupBy("location").agg(avg("NO2").as("avg_NO2"), avg("O3").as("avg_O3"))

  val endTimeWithoutBroadcast = System.nanoTime()
  val durationWithoutBroadcast = (endTimeWithoutBroadcast - startTimeWithoutBroadcast) / 1e9d

  //resultWithoutBroadcast.show()

  println(s"Time taken without broadcasting: $durationWithoutBroadcast seconds")

  // Task 2: Join with Broadcasting
  val startTimeWithBroadcast = System.nanoTime()

  val broadcastData2 = broadcast(data2)
  val joinedDFWithBroadcast = renamedDF.join(broadcast(broadcastData2), "station_name")
  val resultWithBroadcast = joinedDFWithBroadcast.groupBy("location").agg(avg("NO2").as("avg_NO2"), avg("O3").as("avg_O3"))

  val endTimeWithBroadcast = System.nanoTime()
  val durationWithBroadcast = (endTimeWithBroadcast - startTimeWithBroadcast) / 1e9d

  //resultWithBroadcast.show()

  println(s"Time taken with broadcasting: $durationWithBroadcast seconds")

  // Compare Join Aggregation operation: Group By vs Window and Partition By

  // Group By
  val startTimeGroupBy = System.nanoTime()

  val groupByResult = renamedDF.groupBy("station_name").agg(avg("NO2").as("avg_NO2"), avg("O3").as("avg_O3"))

  val endTimeGroupBy = System.nanoTime()
  val durationGroupBy = (endTimeGroupBy - startTimeGroupBy) / 1e9d

  //groupByResult.show()

  println(s"Time taken with Group By: $durationGroupBy seconds")

  // Window and Partition By
  val startTimeWindow = System.nanoTime()

  val windowSpec = Window.partitionBy("station_name").orderBy("Date")
  val windowResult = renamedDF.withColumn("avg_NO2", avg("NO2").over(windowSpec)).withColumn("avg_O3", avg("O3").over(windowSpec))

  val endTimeWindow = System.nanoTime()
  val durationWindow = (endTimeWindow - startTimeWindow) / 1e9d

  //windowResult.show()

  println(s"Time taken with Window and Partition By: $durationWindow seconds")

  // Stop the Spark session
  spark.stop()
}
