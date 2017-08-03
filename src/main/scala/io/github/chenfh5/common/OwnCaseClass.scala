package io.github.chenfh5.common

object OwnCaseClass {

  case class Item(
    id: Int,
    name: String,
    price: Double,
    dt: String
  ) {
    override def toString: String = productIterator.mkString("\t\t")
  }

}
