package com.gelerion.spark.bin.packing.partitioner.utils

import scala.annotation.tailrec

class Rational(n: Int, d: Int) {
  //Scala compiler will compile any code you place in the class body, which isn't part of a field or a method definition, into the primary constructor.
  //----------------------------------------------
  require(d != 0) //preconditions

  //normalize with greatest common divisor
  private val g = gcd(n.abs, d.abs) //will execute before the other two, because it appears first in the source.

  //To access the numerator and denominator on that, you'll need to make them into fields
  val numer: Int = n / g //not accessible from outside the object
  val denom: Int = d / g
  //----------------------------------------------

  def this(n: Int) = this(1, n) // auxiliary constructor

  override def toString: String = numer + "/" + denom

  def + (that: Rational): Rational = {
    new Rational(
      numer * that.denom + that.numer * denom,
      denom * that.denom
    )
  }

  def * (that: Rational): Rational = {
    new Rational(numer * that.numer, denom * that.denom)
  }

  //to write r * 2 instead r * new Rational(2)
  def * (i: Int): Rational = {
    new Rational(numer + i * denom , denom)
  }

  def - (that: Rational): Rational =
    new Rational(
      numer * that.denom - that.numer * denom,
      denom * that.denom
    )

  def - (i: Int): Rational = new Rational(numer - i * denom, denom)

  def value: Double = numer.toDouble / denom

  def lessThan(that: Rational): Boolean = this.numer * that.denom < that.numer * this.denom

  //greatest common divisor
  @tailrec private def gcd(a: Int, b: Int): Int = {
    if(b == 0) a else gcd(b, a % b)
  }
}

object Rational {
  def apply(numerator: Int, denominator: Int) = new Rational(numerator, denominator)
}
