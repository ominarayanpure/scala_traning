import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.log4j.{Level, Logger}
import software.amazon.awssdk.auth.credentials.{DefaultCredentialsProvider, AwsCredentialsProvider}

object s3tomysql {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val credentialsProvider: AwsCredentialsProvider = DefaultCredentialsProvider.builder.build
    // Fetch AWS credentials
    val awsCredentials = credentialsProvider.resolveCredentials()
    // Print the fetched credentials
    println("Access Key ID: " + awsCredentials.accessKeyId())
    println("Secret Access Key: " + awsCredentials.secretAccessKey())
//    println("Session Token: " + awsCredentials.sessionToken()) // This will be null if no session token is provided
    val spark = SparkSession.builder()
      .appName("Spark JSON to MySQL")
      .config("spark.master", "local[*]")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.access.key", awsCredentials.accessKeyId())
      .config("spark.hadoop.fs.s3a.secret.key", awsCredentials.secretAccessKey())
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
      .getOrCreate()

    println("AWS Access Key:", spark.conf.get("spark.hadoop.fs.s3a.access.key"))

    // Read JSON data from S3
    val jsonFilePath = "s3a://scala-training-bucket/demmo/abcd.json"

    val df = spark.read.option("multiline", "true").json(jsonFilePath)
    df.show()

    val jdbcUrl = "jdbc:mysql://35.244.63.238:3306/report_management"
    val dbUser = "root"
    val dbPassword = "Riya@123"
    val tableName = "fruit_info"

    df.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", dbUser)
      .option("password", dbPassword)
      .mode("append")
      .save()

    spark.stop()
  }
}
