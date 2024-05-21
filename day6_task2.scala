import scala.io.Source
import scala.collection.mutable._

@main
def Processing(): Unit = {

  val dataPath = "/Users/onkarramakantnarayanpure/Desktop/scala_training/scala_traning/data.csv"

  val fileSource = Source.fromFile(dataPath)
  var secretList: List[String] = List()

  var lineIndex: Int = 0
  for (line <- fileSource.getLines()) {
    if (lineIndex > 0) {
      val Array(id, fullName, location, income, division) = line.split(",").map(_.trim)
      if(division == "Sales")
        secretList = secretList :+ s"Name : $fullName, Location : $location-"
    }
    lineIndex += 1
  }

  var secretPersonList : ArrayBuffer[String] = ArrayBuffer()

  secretSolver(secretPersonList, secretList, 0, secretList.length)
}


def secretSolver(secretPersonList : ArrayBuffer[String] , secretList: List[String], start: Int, n: Int) : Unit = {
  if(secretPersonList.length == 4) {
//    println("group of 4 individuals:")
    println(secretPersonList.mkString(" "))
    println("//")
    return
  }

  for( i<- start until n){
    secretPersonList += secretList(i)
    secretSolver(secretPersonList, secretList, i+1, n)
    secretPersonList.remove(secretPersonList.length - 1)
  }
}

case class Staff(id: Int, fullName: String, location: String, income: Double, department: String)
case class DepartmentTask(departmentName: String)

