import scala.util.control.Breaks._
import scala.io.StdIn

def printAll(seatMatrix: Array[Array[String]]): Unit = {
  val numRows = seatMatrix.length
  val numCols = seatMatrix(0).length

  for (i <- 0 until numRows) {
    for (j <- 0 until numCols) {
      print(seatMatrix(i)(j) + " ")
      if (j == numCols - 1) println(" ")
    }
  }
}

def assignSeats(seatMatrix: Array[Array[String]], seatNumbers: Array[Int], callback: Array[Array[String]] => Unit): Unit = {
  println("Initial seat matrix:")

  callback(seatMatrix)
  val numRows = seatMatrix.length
  val numSeats = seatNumbers.length

  for (i <- 0 until numSeats) {
    val rowIdx = seatNumbers(i) / numRows
    val colIdx = seatNumbers(i) % numRows - 1

    seatMatrix(rowIdx)(colIdx) = "X"
  }

  println("-------------------------")
  println("Modified seat matrix:")
  callback(seatMatrix)
}

@main
def runBooking(): Unit = {
  val seatMatrix = Array.fill(10, 10)("1")
  seatMatrix(0)(0) = "X"
  seatMatrix(2)(0) = "X"
  seatMatrix(0)(3) = "X"

  var continueBooking = true

  breakable {
    while (continueBooking) {
      println("Enter seat numbers that you want to book:")
      val line = StdIn.readLine("Enter the that are numbers separated by spaces: ")
      val seatNumbers = line.split(" ").map(_.toInt)

      assignSeats(seatMatrix, seatNumbers, printAll)

      val shouldContinue = StdIn.readLine("do you want to continue? (true/false) ")
      if (shouldContinue == "false") break()
    }
  }
}