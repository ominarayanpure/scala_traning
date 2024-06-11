import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object hadooptask {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV to JSON Converter")
      .getOrCreate()

    Class.forName("com.mysql.cj.jdbc.Driver")

    val inputPath = "hdfs://0.0.0.0:9000/employee.csv"
    val outputPath = "hdfs://0.0.0.0:9000/json"

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)

    val filteredDf = df.filter(col("Salary") > 89000)

    filteredDf.write
      .mode("overwrite")
      .json(outputPath)

    filteredDf.write
      .format("jdbc")
      .option("url", "jdbc:mysql://35.244.63.238:3306/report_management")
      .option("dbtable", "employee")
      .option("user", "root")
      .option("password", "Riya@123")
      .mode(SaveMode.Overwrite) // Overwrite existing data
      .save()

    spark.stop()
  }
}