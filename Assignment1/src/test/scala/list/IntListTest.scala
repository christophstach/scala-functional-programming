package list

import list.implementation.SinglyLinkedIntList
import org.scalatest.FunSuite

class IntListTest extends FunSuite {


  test("test object apply") {
    assert(SinglyLinkedIntList(4, 5, 6).head === 4)
  }

  test("test object apply tail") {
    assert(SinglyLinkedIntList(4, 5, 6).tail === SinglyLinkedIntList(5, 6))
  }

  test("testIsEmpty positive") {
    assert(SinglyLinkedIntList().isEmpty === true)
  }

  test("testIsEmpty negative") {
    assert(SinglyLinkedIntList(1, 2).isEmpty === false)
  }

  test("testContains positive") {
    assert(SinglyLinkedIntList(4, 5, 6).contains(4) === true)

  }

  test("testSize empty") {
    assert(SinglyLinkedIntList().size === 0)

  }

  test("testSize nonempty") {
    assert(SinglyLinkedIntList(4, 5, 6).size === 3)

  }

  test("testContains negative") {
    assert(SinglyLinkedIntList(4, 5, 6).contains(42) === false)
  }

  test("testGet") {
    assert(SinglyLinkedIntList(4, 5, 6).get(2) === 6)
  }

  test("testPrepend") {
    assert(SinglyLinkedIntList(4, 5, 6).prepend(3) === SinglyLinkedIntList(3, 4, 5, 6))
  }

  test("testAppend") {
    assert(SinglyLinkedIntList(4, 5, 6).append(7) === SinglyLinkedIntList(4, 5, 6, 7))
  }

  test("testPrefix") {
    assert(SinglyLinkedIntList(4, 5, 6).prefix(SinglyLinkedIntList(1, 2, 3)) === SinglyLinkedIntList(1, 2, 3, 4, 5, 6))
  }

  test("testDelete") {
    assert(SinglyLinkedIntList(4, 5, 6, 6, 7, 6).delete(6) === SinglyLinkedIntList(4, 5, 6, 7, 6))
  }

  test("testDeleteAll") {
    assert(SinglyLinkedIntList(4, 5, 6, 6, 7).deleteAll(6) === SinglyLinkedIntList(4, 5, 7))
  }


  /** ------------------------------------------
    *
    * Exercise 5
    *
    * ------------------------------------------ */

  test("testMap") {
    assert(SinglyLinkedIntList(1, 2, 3).map(x => x * x) === SinglyLinkedIntList(1, 4, 9))
  }

  test("testFilter") {
    assert(SinglyLinkedIntList(1, 2, 3).filter(x => x > 2) === SinglyLinkedIntList(3))
  }

  test("testFoldLeft") {
    assert(SinglyLinkedIntList(1, 2, 3).foldLeft(5)((x, y) => x + y) === 11)
  }

  test("testFoldLeft is left to right") {
    assert(SinglyLinkedIntList(3, 2, 1).foldLeft(5)((x, y) => {
      if (x < y)
        fail()
      else x + y
    }) === 11)
  }

  test("testReduceLeft is left to right") {
    assert(SinglyLinkedIntList(3, 2, 1).reduceLeft((x, y) => {
      if (x < y)
        fail()
      else x + y
    }) === 6)
  }

  /** ------------------------------------------
    *
    * Assignment 1
    *
    * ------------------------------------------ */

  test("testForAll positive") {
    assert(SinglyLinkedIntList(1, 2, 3).forAll(x => x > 0) === true)
  }

  test("testForAll negative") {
    assert(SinglyLinkedIntList(1, 2, 3).forAll(x => x > 3) === false)
  }

  test("testForAll returns on first negative") {
    var c = 0
    SinglyLinkedIntList(1, 2, 3, 4, 5, 6).forAll(x => {
      c += 1
      x < 3
    })
    assert(c == 3)
  }

  test("testFoldRight") {
    assert(SinglyLinkedIntList(1, 2, 3).foldRight(5)((x, y) => x + y) === 11)
  }

  test("testFoldRight is right to left") {
    assert(SinglyLinkedIntList(1, 2, 3).foldRight(5)((x, y) => {
      println()
      if (x > y)
        fail()
      else x + y
    }) === 11)
  }

  test("testReduceRight") {
    assert(SinglyLinkedIntList(1, 2, 3).reduceRight(_ + _) === 6)
  }

  test("testReduceRight is right to left") {
    assert(SinglyLinkedIntList(1, 2, 3).reduceRight((x, y) => {
      if (x > y)
        fail()
      else x + y
    }) === 6)
  }

  test("testInsertionSort") {
    assert(SinglyLinkedIntList(5, 1, 2, 4, 3).insertionSort == SinglyLinkedIntList(1, 2, 3, 4, 5))
  }
  test("testInsertSorted") {
    assert(SinglyLinkedIntList(1, 2, 3, 4).insertSorted(5) === SinglyLinkedIntList(1, 2, 3, 4, 5))
  }

  test("testFoldLeft generic") {
    assert(SinglyLinkedIntList(1, 2, 3).foldLeft(5.0f)((x, y) => x + y) === 11)
  }

  test("testFoldLeft generic 2") {
    assert(SinglyLinkedIntList(1, 2, 3).foldLeft((0,0))((x, y) => (x._1 + y,x._2+1)) === (6,3))
  }

  test("testFoldLeft generic is left to right") {
    assert(SinglyLinkedIntList(3, 2, 1).foldLeft(5.0f)((x, y) => {
      if (x < y)
        fail()
      else x + y
    }) === 11)
  }
}