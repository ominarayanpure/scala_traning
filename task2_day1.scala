import scala.util.control.Breaks._

def findPivot(array: Array[Int], low: Int, high: Int): Int = {
  var start = low
  val pivotValue = array(high)
  for (i <- low until high) {
    if (array(i) <= pivotValue) {
      val temp = array(start)
      array(start) = array(i)
      array(i) = temp
      start = start + 1
    }
  }
  val temp = array(start)
  array(start) = array(high)
  array(high) = temp
  start
}

def quickSort(array: Array[Int], low: Int, high: Int): Unit = {
  if (low <= high) {
    val pivotIndex = findPivot(array, low, high)
    quickSort(array, low, pivotIndex - 1)
    quickSort(array, pivotIndex + 1, high)
  }
}

def mergeSort(array: Array[Int]): Unit = {
  def mergeSortHelper(start: Int, end: Int): Unit = {
    if (start < end) {
      val mid = (start + end) / 2
      mergeSortHelper(start, mid)
      mergeSortHelper(mid + 1, end)
      merge(start, mid, end)
    }
  }

  def merge(start: Int, mid: Int, end: Int): Unit = {
    val left = array.slice(start, mid + 1)
    val right = array.slice(mid + 1, end + 1)
    var i = 0
    var j = 0
    var k = start

    while (i < left.length && j < right.length) {
      if (left(i) <= right(j)) {
        array(k) = left(i)
        i += 1
      } else {
        array(k) = right(j)
        j += 1
      }
      k += 1
    }

    while (i < left.length) {
      array(k) = left(i)
      i += 1
      k += 1
    }

    while (j < right.length) {
      array(k) = right(j)
      j += 1
      k += 1
    }
  }

  mergeSortHelper(0, array.length - 1)
}

def binarySearch(array: Array[Int], target: Int): Int = {
  var low = 0
  var high = array.length - 1
  var mid = 0
  var result = 0
  breakable {
    while (low <= high) {
      mid = low + (high - low) / 2
      if (array(mid) == target) {
        result = mid
        break()
      }
      if (array(mid) < target) low = mid + 1
      else high = mid - 1
    }
  }
  result
}

def getAlgorithm(algorithmType: String): Either[Array[Int] => Unit, (Array[Int], Int) => Int] = {
  algorithmType match {
    case "bubble" => Left((arr: Array[Int]) => {
      val n = arr.length
      for (i <- 0 until n - 1) {
        for (j <- 0 until n - i - 1) {
          if (arr(j) > arr(j + 1)) {
            val temp = arr(j)
            arr(j) = arr(j + 1)
            arr(j + 1) = temp
          }
        }
      }
    })
    case "insertion" => Left((arr: Array[Int]) => {
      for (i <- 1 until arr.length) {
        val key = arr(i)
        var j = i - 1
        while (j >= 0 && arr(j) > key) {
          arr(j + 1) = arr(j)
          j = j - 1
        }
        arr(j + 1) = key
      }
    })
    case "quick" => Left((arr: Array[Int]) => {
       quickSort(arr, 0, arr.length - 1)
    })
    case "merge" => Left((arr: Array[Int]) => {
      mergeSort(arr)
    })
    case "binarySearch" => Right((arr: Array[Int], target: Int) => binarySearch(arr, target))
  }
}

@main
def executeTasks(): Unit = {
    var array = Array(2, 33, 5, 10)
    val sortingAlgorithm = getAlgorithm("insertion")

    sortingAlgorithm match {
      case Left(sort) => {
        println("Sorting using insertion sort algorithm")
        println(array.mkString(" "))
        sort(array)
        println(array.mkString(" "))
      }
      case Right(_) => println("No sorting algorithm chosen")
    }

    val searchingAlgorithm = getAlgorithm("binarySearch")
    searchingAlgorithm match {
      case Right(search) => {
        println("Searching using binary search algorithm")
        val index = search(array, 5)
        println(s"Index of the target is $index")
      }
      case Left(_) => println("No searching algorithm chosen")
    }
}
