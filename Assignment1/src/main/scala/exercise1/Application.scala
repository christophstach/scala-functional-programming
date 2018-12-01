package exercise1

object Application {

  def main(args: Array[String]): Unit = {
    println("Hier startet das erste Scala Programm!")
    val x = new Rational(2, 5)
    val y = new Rational(3, 4)
    val z = x.max(y)

    // throws a AssertionError if the condition is not met
    assert(z == y)
  }
}

