import scala.io.Source
import scala.collection.mutable
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.sql.SQLIntegrityConstraintViolationException

@main
def processEmployeeData(): Unit = {
  // Path to the data file
  val dataPath = "/Users/onkarramakantnarayanpure/Desktop/scala_training/scala_traning/data.csv"

  // Read data from the file
  val fileSource = Source.fromFile(dataPath)
  var employees: List[Employee] = List()
  var uniqueDepartments: mutable.Set[String] = mutable.Set()
  var departmentMap: mutable.Map[String, mutable.ArrayBuffer[Employee]] = mutable.Map()

  var lineIndex: Int = 0
  for (line <- fileSource.getLines()) {
    if (lineIndex > 0) { // Skip header line
      val Array(id, fullName, location, income, department) = line.split(",").map(_.trim)
      val employee: Employee = Employee(id.toInt, fullName, location, income.toDouble, department)
      uniqueDepartments += department
      employees = employees :+ employee
      // Add employee to the department map
      if (departmentMap.contains(department)) {
        departmentMap(department) += employee
      } else {
        departmentMap(department) = mutable.ArrayBuffer(employee)
      }
    }
    lineIndex += 1
  }

  println("Unique Departments:")
  println(uniqueDepartments.mkString(", "))

  // Connect to the database
  Class.forName("com.mysql.cj.jdbc.Driver")
  val url = "jdbc:mysql://hadoop-server.mysql.database.azure.com:3306/onkar__database"
  val username = "sqladmin"
  val password = "Password@12345"
  val connection: Connection = DriverManager.getConnection(url, username, password)

  try {
    val statement: Statement = connection.createStatement()

    // Create Department table if not exists
    val createDepartmentSQL =
      """
        |CREATE TABLE IF NOT EXISTS Department (
        |  id INT AUTO_INCREMENT PRIMARY KEY,
        |  departmentName VARCHAR(100) UNIQUE
        |)
        |""".stripMargin
    statement.execute(createDepartmentSQL)

    // Create Employee table if not exists
    val createEmployeeSQL =
      """
        |CREATE TABLE IF NOT EXISTS Employee (
        |  id INT PRIMARY KEY,
        |  fullName VARCHAR(100),
        |  location VARCHAR(100),
        |  income DOUBLE,
        |  departmentName VARCHAR(100),
        |  FOREIGN KEY (departmentName) REFERENCES Department(departmentName)
        |)
        |""".stripMargin
    statement.execute(createEmployeeSQL)

    // Insert unique departments into Department table
    val insertDepartmentSQL = "INSERT IGNORE INTO Department (departmentName) VALUES (?)"
    val departmentPreparedStatement = connection.prepareStatement(insertDepartmentSQL)
    uniqueDepartments.foreach { department =>
      departmentPreparedStatement.setString(1, department)
      departmentPreparedStatement.executeUpdate()
    }

    // Insert employee data into Employee table
    val insertEmployeeSQL = "INSERT INTO Employee (id, fullName, location, income, departmentName) VALUES (?, ?, ?, ?, ?)"
    val employeePreparedStatement = connection.prepareStatement(insertEmployeeSQL)
    employees.foreach { employee =>
      employeePreparedStatement.setInt(1, employee.id)
      employeePreparedStatement.setString(2, employee.fullName)
      employeePreparedStatement.setString(3, employee.location)
      employeePreparedStatement.setDouble(4, employee.income)
      employeePreparedStatement.setString(5, employee.department)
      employeePreparedStatement.executeUpdate()
    }

    println("Data insertion into database completed successfully.")

    // Print the department map
    println("\nDepartment-wise Employee Details:")
    departmentMap.foreach { case (department, employees) =>
      println(s"Department: $department")
      employees.foreach { employee =>
        println(s"  ID: ${employee.id}, Name: ${employee.fullName}, Location: ${employee.location}, Income: ${employee.income}")
      }
    }

  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    // Close the database connection
    connection.close()
  }
}

// Define case classes
case class Employee(id: Int, fullName: String, location: String, income: Double, department: String)
case class DepartmentTask_2(departmentName: String)
