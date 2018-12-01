package assignment1

import list.implementation.SinglyLinkedIntList
import org.scalatest.FunSuite

class ProblemsTest extends FunSuite {

  test("testMinBagsCounts") {
    assert(Problems.minBagsCount(10, SinglyLinkedIntList(5, 4, 12)) === 3)
  }

  test("testCountChange") {
    assert(Problems.countChange(4, SinglyLinkedIntList(1, 2)) === 3)
  }

  test("testShouldTakeEvenAddresses") {
    //36,42,52 even => 14 (8 + 3x2)
    //41, 43, 45, 47, 51 odd => 15 (5 + 5x2)
    val isEvenBetter = Problems.shouldTakeEvenAddresses(SinglyLinkedIntList(45, 47,51, 42, 41, 36, 52, 43))
    assert(isEvenBetter === true)
  }
  test("testShouldTakeEvenAddresses 2") {
    //40,42,52 even => 12 (6 + 3x2)
    //41, 83 odd => 25 (21 + 2x2)
    val isEvenBetter = Problems.shouldTakeEvenAddresses(SinglyLinkedIntList(40, 41, 42, 52, 83))
    assert(isEvenBetter === true)
  }

  test("testShouldTakeEvenAddressesExtended") {
    //36,42,52 even => 14 (8 + 3x2)
    //41, 43, 45, 47, 51 odd => 13 (5 + 3x2 +2x1)
    val isEvenBetter = Problems.shouldTakeEvenAddressesExtended(SinglyLinkedIntList(45, 47,51, 42, 41, 36, 52, 43))
    assert(isEvenBetter === false)
  }
}
