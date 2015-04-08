package io.toktok.model

import com.novus.salat.annotations.raw.Salat
import unstable.macros.{InjectedTypeHint, TypeHint}

@Salat
trait Event {

  val _typeHint: TypeHint = InjectedTypeHint(this.getClass.getCanonicalName)

}
