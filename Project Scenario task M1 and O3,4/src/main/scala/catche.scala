import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, DefaultCredentialsProvider}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object persistVSnonpersist extends App {

  // Set log level to reduce Spark logs in the console
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Get AWS credentials
  val credentialsProvider: AwsCredentialsProvider = DefaultCredentialsProvider.builder.build
  val awsCredentials = credentialsProvider.resolveCredentials()

  // Create Spark session with necessary configurations
  def createSparkSession(): SparkSession = {
    SparkSession.builder
      .appName("Persist vs Non-persist")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1") // Set the bind address
      .config("spark.cassandra.connection.host", "cassandra.eu-north-1.amazonaws.com")
      .config("spark.cassandra.connection.port", "9142")
      .config("spark.cassandra.connection.ssl.enabled", "true")
      .config("spark.cassandra.auth.username", "casssandra-user-at-058264511862")
      .config("spark.cassandra.auth.password", "w3Ya6f0gvWM2Wt3UBycR9uoT1MPCl4Rq776ua9DlpX4WDNVEbx/cfOhpRcA=")
      .config("spark.cassandra.input.consistency.level", "LOCAL_QUORUM")
      .config("spark.cassandra.connection.ssl.trustStore.path", "/Users/purvigupta/cassandra_truststore.jks")
      .config("spark.cassandra.connection.ssl.trustStore.password", "abc@123")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.access.key", awsCredentials.accessKeyId())
      .config("spark.hadoop.fs.s3a.secret.key", awsCredentials.secretAccessKey())
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
      .getOrCreate()
  }

  // Initialize the Spark session
  val spark: SparkSession = createSparkSession()

  // Read CSV file from S3
  val csvFile = "s3a://scala-training-bucket/demmo1/zaragoza_data.csv"
  val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvFile)

  // Display first 5 rows
  df.show(5)

  // Rename columns with invalid characters
  val renamedDF = df
    .withColumnRenamed("Wind-Speed (U)", "Wind_Speed_U")
    .withColumnRenamed("Wind-Speed (V)", "Wind_Speed_V")
    .withColumnRenamed("Dewpoint Temp", "Dewpoint_Temp")
    .withColumnRenamed("Total Percipitation", "Total_Precipitation")
    .withColumnRenamed("Relative Humidity", "Relative_Humidity")
    .withColumnRenamed("Vegitation (High)", "Vegitation_High")
    .withColumnRenamed("Vegitation (Low)", "Vegitation_Low")
    .withColumnRenamed("Soil Temp","Soil_Temp")

  renamedDF.show(5)

  // Implementing without cache and persist
  val startTimeWithoutCache = System.nanoTime()

  // Perform operations
  val avgNo2 = renamedDF.agg(avg("NO2")).first().getDouble(0)
  val avgO3 = renamedDF.agg(avg("O3")).first().getDouble(0)

  val endTimeWithoutCache = System.nanoTime()
  val durationWithoutCache = (endTimeWithoutCache - startTimeWithoutCache) / 1e9d

  //println(s"Average NO2: $avgNo2, Average O3: $avgO3")
  //println(s"Time taken without caching: $durationWithoutCache seconds")

  // Cache the DataFrame
  renamedDF.cache()

  val startTimeWithCache = System.nanoTime()

  // Perform operations again with caching
  val avgNo2Cached = renamedDF.agg(avg("NO2")).first().getDouble(0)
  val avgO3Cached = renamedDF.agg(avg("O3")).first().getDouble(0)

  val endTimeWithCache = System.nanoTime()
  val durationWithCache = (endTimeWithCache - startTimeWithCache) / 1e9d

  //println(s"Average NO2 with cache: $avgNo2Cached, Average O3 with cache: $avgO3Cached")
  //println(s"Time taken with caching: $durationWithCache seconds")

  // Persist the DataFrame to disk
  renamedDF.write.mode("overwrite").parquet("s3a://scala-training-bucket/cached_zaragoza_data.parquet")

  // Read the persisted DataFrame
  val startTimeWithPersistence = System.nanoTime()

  val dfPersisted = spark.read.parquet("s3a://scala-training-bucket/cached_zaragoza_data.parquet")
  val avgNo2Persisted = dfPersisted.agg(avg("NO2")).first().getDouble(0)
  val avgO3Persisted = dfPersisted.agg(avg("O3")).first().getDouble(0)

  val endTimeWithPersistence = System.nanoTime()
  val durationWithPersistence = (endTimeWithPersistence - startTimeWithPersistence) / 1e9d

  //println(s"Average NO2 with persistence: $avgNo2Persisted, Average O3 with persistence: $avgO3Persisted")
  //println(s"Time taken with persistence: $durationWithPersistence seconds")

  println(s"Time taken without caching: $durationWithoutCache seconds")
  println(s"Time taken with caching: $durationWithCache seconds")
  println(s"Time taken with persistence: $durationWithPersistence seconds")

  // Stop the Spark session
  spark.stop()
}
