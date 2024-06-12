import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

object hdfs_to_database {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV to JSON Transformer")
      .master("local[*]")
      .getOrCreate()

    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    import spark.implicits._

    case class Department(departmentID: String, departmentName: String)
    case class Employee(departmentID: String, employeeID: String, employeeName: String)

    val departmentData = Seq(
      Row("dept1", "Engineering"),
      Row("dept2", "Sales")
    )

    val employeeData = Seq(
      Row("dept1", "emp1", "Alice"),
      Row("dept2", "emp2", "Bob"),
      Row("dept2", "emp3", "Charlie")
    )

    val departmentSchema = new StructType()
      .add("departmentID", StringType)
      .add("departmentName", StringType)

    val employeeSchema = new StructType()
      .add("departmentID", StringType)
      .add("employeeID", StringType)
      .add("employeeName", StringType)

    val dfDepartment = spark.createDataFrame(spark.sparkContext.parallelize(departmentData), departmentSchema)
    val dfEmployee = spark.createDataFrame(spark.sparkContext.parallelize(employeeData), employeeSchema)

    val employeeAggregatedDF = dfEmployee.groupBy("departmentID")
      .agg(collect_list(struct($"employeeID", $"employeeName")).alias("employees"))

    val resultDF = dfDepartment.as("d").join(employeeAggregatedDF.as("e"), $"d.departmentID" === $"e.departmentID")
      .select(to_json(collect_list(struct("d.departmentID", "d.departmentName", "e.employees"))).alias("json"))

    println(resultDF.head().getString(0))
  }
}
