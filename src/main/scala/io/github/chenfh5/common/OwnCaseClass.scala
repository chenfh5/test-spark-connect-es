package io.github.chenfh5.common

object OwnCaseClass {

  case class Item(
    id: Long,
    name: String,
    price: Double,
    dt: String
  ) {
    override def toString: String = productIterator.mkString("\t\t")
  }

  case class SuggestJson(
    item_id: Long,
    item_name_suggester: List[String]
  )

  case class NormalJson(
    item_id: Long,
    item_name: String,
    item_price: Double,
    shop_name: String
  )

}
