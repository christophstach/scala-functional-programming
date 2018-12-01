package exercise5

import list.implementation.SinglyLinkedIntList
import org.scalatest.FunSuite

class ProblemsTest extends FunSuite {

  test("testSumOddNumbers") {
    assert(Problems.sumOddNumbers(SinglyLinkedIntList(1, 2, 3, 4, 5)) === 9)
  }

  test("testCountEvenNumbers") {
    assert(Problems.countEvenNumbers(SinglyLinkedIntList(1, 2, 3, 4, 5)) === 2)
  }

  test("testMultiplyAndFilterEven") {
    assert(Problems.multiplyAndFilterEven(SinglyLinkedIntList(1, 2, 3), 3) === SinglyLinkedIntList(6))
  }

  test("testFindMin") {
    assert(Problems.findMin(SinglyLinkedIntList(3, 5, 1, 2, 3, 4, 5)) === 1)
  }
}
