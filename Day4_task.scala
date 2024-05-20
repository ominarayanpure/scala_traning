import scala.io.Source

@main
def run(): Unit = {

  val pathToFile = "/Users/onkarramakantnarayanpure/Desktop/scala_training/scala_traning/data.csv"

  val sourceBuffer = Source.fromFile(pathToFile)
  var employeeList: List[Staff] = List()
  var lineIndex: Int = 0
  for (line <- sourceBuffer.getLines) {
    if (lineIndex > 0) {
      val Array(serialNo, fullName, location, income, division) = line.split(",").map(_.trim)
      val staffMember: Staff = Staff(serialNo.toInt, fullName, location, income.toDouble, division)
      employeeList = employeeList :+ staffMember
    }
    lineIndex += 1
  }

  val highEarningSalesStaff = employeeList.filter(staff => staff.income >= 50000 && staff.division.equalsIgnoreCase("sales"))
  println("\nSales Department Staff with Salary Above 50000")
  highEarningSalesStaff.foreach(println)

  val capitalizedNamesStartingWithJ = employeeList.map(staff => staff.fullName.toUpperCase).filter(name => name.startsWith("J"))
  println("\nStaff Names in Uppercase Starting with J")
  capitalizedNamesStartingWithJ.foreach(println)

  val employeesGroupedByDivision = employeeList.groupBy(_.division)

  println("\nStatistics by Department")
  val divisionStatistics = employeesGroupedByDivision.view.mapValues { employees =>
    val totalStaff = employees.length
    val combinedSalary = employees.map(_.income).sum
    val meanSalary = combinedSalary / employees.length.toDouble
    (totalStaff, combinedSalary, meanSalary)
  }

  divisionStatistics.foreach { case (division, (totalStaff, combinedSalary, meanSalary)) =>
    println(s"Division: $division, Total Staff: $totalStaff, Combined Salary: $combinedSalary, Average Salary: $meanSalary")
  }
  sourceBuffer.close()
}

case class Staff(sno: Int, fullName: String, location: String, income: Double, division: String)
