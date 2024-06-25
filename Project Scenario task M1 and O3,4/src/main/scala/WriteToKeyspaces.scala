import org.apache.spark.sql.{DataFrame, SparkSession}
import software.amazon.awssdk.auth.credentials.{DefaultCredentialsProvider, AwsCredentialsProvider}


object WriteToKeyspaces extends App {
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
    .format("csv")
    .option("header", "true")
    .load("s3a://scala-training-bucket/demmo1/zaragoza_data.csv")

  println(aqi.schema.toDDL)

  val cols:Array[String]=aqi.columns
  var finalcol:Array[String]=cols.map(p=>p.replace(' ','_'))
  finalcol=finalcol.map(p=>p.replace(')','_'))
  finalcol=finalcol.map(p=>p.replace('(','_'))
  finalcol=finalcol.map(p=>p.replace('-','_'))
  finalcol=finalcol.map(p=>p.replace('.','_'))
  var aqi_new = aqi.toDF(finalcol:_*)
  //aqi_new = aqi.select([F.col(x).alias(x.replace(' ', '_')) for x in aqi.columns])
  aqi_new.printSchema()
  //aqi.printSchema()

  aqi_new.write
    .format("org.apache.spark.sql.cassandra")
    .option("keyspace", "tutorialkeyspace")
    .option("table", "zaragoza_data")
    .mode("append")
    .save()

  spark.stop()
}

