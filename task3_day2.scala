import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn
import scala.util.control.Breaks.{break, breakable}

case class Employee(srno: Int, name: String, city: String)

class Department(val name: String, val employees: ArrayBuffer[Employee], val subUnits: ArrayBuffer[Department]) {
  def addEmployee(employee: Employee, departmentName: String): Boolean = {
    if (name == departmentName) {
      employees += employee
      true
    } else {
      subUnits.exists(_.addEmployee(employee, departmentName))
    }
  }

  def showHierarchy(indent: String = ""): Unit = {
    println(s"$indent$name")
    employees.foreach(e => println(s"$indent  (${e.srno}, ${e.name}, ${e.city})"))
    subUnits.foreach(_.showHierarchy(indent + "  "))
  }

  override def toString: String = s"Department(name=$name, employees=${employees.mkString(", ")}, subUnits=${subUnits.mkString(", ")})"
}

@main def task1day3 = {
  val paymentDepartment = new Department("Payments", ArrayBuffer[Employee](), ArrayBuffer[Department]())
  val financeDepartment = new Department("Finance", ArrayBuffer[Employee](), ArrayBuffer[Department](paymentDepartment))

  val marketingDepartment = new Department("Marketing", ArrayBuffer[Employee](), ArrayBuffer[Department]())
  val advertisementDepartment = new Department("Advertisements", ArrayBuffer[Employee](), ArrayBuffer[Department]())
  val saleManagementDepartment = new Department("SalesManagement", ArrayBuffer[Employee](), ArrayBuffer[Department]())

  val salesDepartment = new Department("Sales", ArrayBuffer[Employee](), ArrayBuffer[Department](marketingDepartment, advertisementDepartment, saleManagementDepartment))

  val orgDepartment = new Department("Organisation", ArrayBuffer[Employee](), ArrayBuffer[Department](financeDepartment, salesDepartment))

  breakable {
    while (true) {
      println("Enter sr no, name, city, department (or type 'exit' to quit):")
      val line = StdIn.readLine().trim

      if (line == "exit") break()

      val parts = line.split(",").map(_.trim)
      if (parts.length != 4) {
        println("Invalid input. Please enter sr no, name, city, and department separated by commas.")
      } else {
        try {
          val srno = parts(0).toInt
          val name = parts(1)
          val city = parts(2)
          val departmentName = parts(3)

          val employee = Employee(srno, name, city)
          val added = orgDepartment.addEmployee(employee, departmentName)
          if (!added) {
            println(s"Department '$departmentName' not found.")
          } else {
            println(s"Added employee $employee to department $departmentName.")
          }
        } catch {
          case _: NumberFormatException => println("Invalid sr no. It should be an integer.")
        }
      }

      // Print the organization hierarchy after each addition
      println("Organization Hierarchy:")
      orgDepartment.showHierarchy()
    }
  }
}
