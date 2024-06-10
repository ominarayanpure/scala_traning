import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.auth.credentials.{DefaultCredentialsProvider, AwsCredentialsProvider}

object mysqltos3 {
  def main(args: Array[String]): Unit = {
    val credentialsProvider: AwsCredentialsProvider = DefaultCredentialsProvider.builder.build
    // Fetch AWS credentials
    val awsCredentials = credentialsProvider.resolveCredentials()
    // Print the fetched credentials
    println("Access Key ID: " + awsCredentials.accessKeyId())
    println("Secret Access Key: " + awsCredentials.secretAccessKey())
//    println("Session Token: " + awsCredentials.sessionToken()) // This will be null if no session token is provided
    val spark = SparkSession.builder()
      .appName("JDBC Example")
      .config("spark.master", "local")
      .config("spark.hadoop.fs.s3a.access.key", awsCredentials.accessKeyId())
      .config("spark.hadoop.fs.s3a.secret.key", awsCredentials.secretAccessKey())
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
      .getOrCreate()

    // Assuming you have a JDBC connection URL, username, and password
    val url = "jdbc:mysql://35.244.63.238:3306/report_management"
    val tableName = "reports"

    // Specify the JDBC connection properties
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "root")
    connectionProperties.setProperty("password", "Riya@123")

    // Read data from the JDBC data source
    val jdbcDF = spark.read
      .jdbc(url, tableName, connectionProperties)

    // Now you can use jdbcDF to perform further operations like select, filter, etc.
    jdbcDF.show()


    val csvPath = "s3a://scala-training-bucket/demmo"
    jdbcDF.write
      .mode("overwrite")
      .option("header", "true")
      .csv(csvPath)
    spark.stop()
  }
}
