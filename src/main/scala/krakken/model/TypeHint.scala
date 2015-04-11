package krakken.model

import scala.reflect.ClassTag

/**
 * Created by ernest on 4/11/15.
 */
object TypeHint{

  def apply[T](implicit t: ClassTag[T]):TypeHint =
    new TypeHint {
      override val hint: String = t.runtimeClass.getSimpleName
    }
}

trait TypeHint{

  val hint: String

  override def toString: String = s"TypeHint[$hint]"

  override def equals(obj: scala.Any): Boolean = obj match {
    case t: TypeHint ⇒ t.hint == hint
    case any ⇒ false
  }
}