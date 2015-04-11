package krakken.model

import com.novus.salat.annotations.raw.Salat

@Salat
trait Command {

  val entityId: SID = ""
  val typeHint: TypeHint = InjectedTypeHint(this.getClass.getCanonicalName)

}
