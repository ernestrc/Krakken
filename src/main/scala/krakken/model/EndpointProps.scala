package krakken.model

import akka.actor.ActorSystem
import krakken.http.{KrakkenEndpoint, CQRSEndpoint}

import scala.reflect.ClassTag

abstract class EndpointProps(val clazz: Class[_], val args: Any*) {

  def boot: (ActorSystem) ⇒ KrakkenEndpoint

}

object EndpointProps{

  def apply[T <: KrakkenEndpoint : ClassTag](args: Any*): EndpointProps =
    new EndpointProps(implicitly[ClassTag[T]].runtimeClass){
      override def boot = { implicit sys ⇒
        clazz.getConstructors()(0).newInstance(sys, args).asInstanceOf[KrakkenEndpoint]
      }
    }

  def apply[T <: KrakkenEndpoint : ClassTag]: EndpointProps =
    new EndpointProps(implicitly[ClassTag[T]].runtimeClass){
      override def boot = { implicit sys ⇒
        clazz.getConstructors()(0).newInstance(sys).asInstanceOf[KrakkenEndpoint]
      }
    }

}
