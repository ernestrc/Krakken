package krakken.model

import com.novus.salat.annotations.raw.Salat

@Salat
trait Event {

  val _typeHint: TypeHint = InjectedTypeHint(this.getClass.getCanonicalName)

}
