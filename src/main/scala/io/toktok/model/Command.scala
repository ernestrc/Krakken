package io.toktok.model

import com.novus.salat.annotations.raw.Salat
import unstable.macros.{InjectedTypeHint, TypeHint}

@Salat
trait Command {

  val entityId: SID = ""
  val typeHint: TypeHint = InjectedTypeHint(this.getClass.getCanonicalName)

}
