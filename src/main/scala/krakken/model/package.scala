package krakken

import com.novus.salat.TypeHint
import com.novus.salat._
import krakken.model.TypeHint

package object model {

  implicit val ctx = new Context {
    val name = "Always Hint"
    override val typeHintStrategy = StringTypeHintStrategy(when = TypeHintFrequency.Always)
  }

  type FromHintGrater[T <: AnyRef] = PartialFunction[TypeHint, Grater[_ <: T]]

  /**
   * Type alias representing a string id that comes from an anchor's MongoDBObject Id
   */
  type SID = String
}
