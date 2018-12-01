package exercise1

class Rational(numerator: Int, denominator: Int) {

  def this(denom: Int) = this(1, denom)

  override def toString: String = numerator + "/" + denominator

  require(denominator != 0, "Denominator muss != 0 sein") // throws a IllegalArgumentException if the condition is not met
  println("Eine Rationale Zahl wurde erzeugt....") // is part of the constructor

  def num: Int = numerator // so the numerator is accessible from other classes
  def denom: Int = denominator // so the numerator is accessible from other classes
  def value: Double = num.toDouble / denom // converts to floating point number

  def max(other: Rational): Rational = {
    if (numerator / denominator < other.num / other.denom) this else other
  }

  def add(other: Rational): Rational = {

    throw new NotImplementedError()
  }

  def mul(other: Rational): Rational = {
    //TODO
    throw new NotImplementedError()
  }

  def sub(other: Rational): Rational = {
    //TODO
    throw new NotImplementedError()
  }

  def neg(other: Rational): Rational = {
    //TODO
    throw new NotImplementedError()
  }
}
