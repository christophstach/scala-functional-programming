package list.implementation

import list.traits.IntList

case class Cons(head: Int, tail: IntList) extends SinglyLinkedIntList {
}