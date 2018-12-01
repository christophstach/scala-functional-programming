package exercise2

object Application {
  def main(args: Array[String]): Unit = {
    var foundSmallestNumber = false
    val maxEvenlyDivisibleNumber = 20
    var currentNumber = maxEvenlyDivisibleNumber + 1

    while (!foundSmallestNumber) {
      foundSmallestNumber = (2 to maxEvenlyDivisibleNumber)
        .forall(i => currentNumber % i == 0)

      if (foundSmallestNumber) {
        println(s"Found smallest number ${currentNumber} evenly divisible by all number until ${maxEvenlyDivisibleNumber}")
      } else {
        currentNumber += 1
      }
    }
  }
}
