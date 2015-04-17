package krakken

import com.novus.salat.Grater

/**
 * Created by ernest on 4/4/15.
 */
package object model {

  type FromHintGrater[T <: AnyRef] = PartialFunction[TypeHint, Grater[_ <: T]]

  /**
   * Type alias representing a string id that comes from an anchor's MongoDBObject Id
   */
  type SID = String
}
