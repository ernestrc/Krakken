package krakken.model

import com.novus.salat.annotations.raw.Salat

@Salat
trait Event {
  val entityId: SID
}

@Salat
trait Anchor extends Event{
  val entityId: SID = "1"
}