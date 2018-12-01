package list.implementation

import list.traits.IntList

/**
  * A companion object for the singly linked list.
  * This enables creating lists list this: val list = SinglyLinkedIntList(1,2,3)
  * which results in Cons(1,Cons(2,Cons(3,Empty))))
  */
object SinglyLinkedIntList {


  /** The apply function is a special function in scala.
    * It can be invoked with SinglyLinkedIntList.apply(args) or simply SinglyLinkedIntList(args).
    * This particular implementation of it is also a variadic function, i.e.
    * a function which accepts one or more arguments of the same type (integers) as parameters.
    */
  //inside this method xs is of type Seq[int]
  def apply(xs: Int*): SinglyLinkedIntList = xs match {
    case Seq() => Empty
    //: _* results in the sequence being passed as multiple parameters - (1,2,3) instead of Seq[Int]{1,2,3}
    case _ => Cons(xs.head, SinglyLinkedIntList(xs.tail: _*))
  }
}

abstract class SinglyLinkedIntList extends IntList {

  override def isEmpty = false

  override def get(index: Int): Int = index match {
    case 0 => head
    case i => tail.get(i - 1)
  }

  override def append(elem: Int): IntList =
    Cons(head, tail.append(elem))

  override def contains(elem: Int): Boolean =
    if (elem == head) true
    else tail.contains(elem)

  override def prepend(elem: Int): IntList =
    Cons(elem, this)

  override def delete(elem: Int): IntList = tail match {
    case Empty => if (head != elem) Cons(head, Empty) else Empty
    case _ => if (head != elem) Cons(head, tail.delete(elem)) else tail
  }

  override def deleteAll(elem: Int): IntList = tail match {
    case Empty => if (head != elem) Cons(head, Empty) else Empty
    case _ => if (head != elem) Cons(head, tail.deleteAll(elem)) else tail.deleteAll(elem)
  }

  override def prefix(other: IntList): IntList = other match {
    case Empty => Cons(head, tail)
    case _ => Cons(other.head, prefix(other.tail))
  }

  override def size: Int =
    tail.size + 1

  /** ------------------------------------------
    *
    * Exercise 5
    *
    * ------------------------------------------ */


  override def map(mapFunc: Int => Int): IntList = tail match {
    case Empty => Cons(mapFunc(head), Empty)
    case _ => Cons(mapFunc(head), tail.map(mapFunc))
  }


  override def filter(filterFunc: Int => Boolean): IntList = tail match {
    case Empty => if (filterFunc(head)) Cons(head, Empty) else Empty
    case _ => if (filterFunc(head)) Cons(head, tail.filter(filterFunc)) else tail.filter(filterFunc)
  }

  override def foldLeft(initial: Int)(reduceFunc: (Int, Int) => Int): Int = tail match {
    case Empty => reduceFunc(initial, head)
    case _ => tail.foldLeft(reduceFunc(initial, head))(reduceFunc)
  }


  override def reduceLeft(reduceFunc: (Int, Int) => Int): Int = tail match {
    case Empty => head
    case _ => reduceFunc(tail.reduceRight(reduceFunc), head)
  }

  /** ------------------------------------------
    *
    * Assignment 1
    *
    * ------------------------------------------ */

  override def forAll(predicateFunc: Int => Boolean): Boolean = tail match {
    case Empty => predicateFunc(head)
    case _ => if (predicateFunc(head)) tail.forAll(predicateFunc) else false
  }

  override def foldRight(initial: Int)(reduceFunc: (Int, Int) => Int): Int = tail match {
    case Empty => reduceFunc(head, initial)
    case _ => tail.foldRight(reduceFunc(head, initial))(reduceFunc)
  }

  override def reduceRight(reduceFunc: (Int, Int) => Int): Int = tail match {
    case Empty => head
    case _ => reduceFunc(head, tail.reduceRight(reduceFunc))
  }

  override def insertionSort: IntList =
    tail.foldLeft[IntList](Cons(head, Empty))((acc, curr) => acc.insertSorted(curr))

  override def insertSorted(elem: Int): IntList = tail match {
    case Empty =>
      if (head <= elem) Cons(head, Cons(elem, Empty))
      else Cons(elem, Cons(head, Empty))
    case _ =>
      if (head <= elem) Cons(head, tail.insertSorted(elem))
      else Cons(elem, Cons(head, tail))
  }

  override def foldLeft[A](initial: A)(reduceFunc: (A, Int) => A): A = tail match {
    case Empty => reduceFunc(initial, head)
    case _ => tail.foldLeft(reduceFunc(initial, head))(reduceFunc)
  }
}