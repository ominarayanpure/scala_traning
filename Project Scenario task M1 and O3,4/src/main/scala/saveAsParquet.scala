
import WriteToKeyspaces.awsCredentials
import org.apache.spark.sql.{DataFrame, SparkSession}
import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, DefaultCredentialsProvider}


object saveAsParquet extends App {
  val credentialsProvider: AwsCredentialsProvider = DefaultCredentialsProvider.builder.build
  val awsCredentials = credentialsProvider.resolveCredentials()
  def createSparkSession(): SparkSession = {
    SparkSession.builder
      .appName("Writing data to Apache Cassandra")
      .master("local[*]")
      .config("spark.cassandra.connection.host", "cassandra.ap-south-1.amazonaws.com")
      .config("spark.cassandra.connection.port", "9142")
      .config("spark.cassandra.connection.ssl.enabled", "true")
      .config("spark.cassandra.auth.username", "apple-scala-project-at-381491937787")
      .config("spark.cassandra.auth.password", "9MtubnmAGiBdlt+fH3JFMUv+173xUTKKHHzCDgVnyUeaDovh2OaZjsSmAdc=")
      .config("spark.cassandra.output.consistency.level", "LOCAL_QUORUM")
      .config("spark.cassandra.connection.ssl.trustStore.path", "/Users/onkarramakantnarayanpure/cassandra_truststore.jks")
      .config("spark.cassandra.connection.ssl.trustStore.password", "Riya@123")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.access.key", awsCredentials.accessKeyId())
      .config("spark.hadoop.fs.s3a.secret.key", awsCredentials.secretAccessKey())
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
      .getOrCreate()
  }


  val spark: SparkSession = createSparkSession()

  // Define a case class corresponding to your schema

  val aqi = spark.read
    .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "zaragoza_data", "keyspace" -> "tutorialkeyspace"))
    .load()



  aqi.printSchema()
  aqi.write.format("parquet").mode("overwrite").save("s3a://scala-training-bucket/demmo/zaragoza_data_parquet")

  spark.stop()
}

