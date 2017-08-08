package io.github.chenfh5.common

object OwnImplicits {

  implicit def anyToInt(x: Option[AnyRef]) = x.getOrElse(-1).asInstanceOf[Number].intValue()

  implicit def anyToLong(x: Option[AnyRef]) = x.getOrElse(-1L).asInstanceOf[Number].longValue()

  implicit def anyToDouble(x: Option[AnyRef]) = x.getOrElse(-1.0).asInstanceOf[Number].doubleValue()

  implicit def anyToString(x: Option[AnyRef]) = x.getOrElse("-1").asInstanceOf[String]
}
