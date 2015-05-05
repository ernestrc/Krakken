package krakken.http

import akka.actor.{ActorContext, ActorSystem}

import scala.reflect.ClassTag

abstract class EndpointProps(val clazz: Class[_], val args: Any*) {

  def boot: (ActorContext) ⇒ KrakkenEndpoint

}

object EndpointProps{

  def apply[T <: KrakkenEndpoint : ClassTag](args: Any*): EndpointProps =
    new EndpointProps(implicitly[ClassTag[T]].runtimeClass){
      override def boot = { implicit ctx ⇒
        clazz.getConstructors()(0).newInstance(ctx, args).asInstanceOf[KrakkenEndpoint]
      }
    }

  def apply[T <: KrakkenEndpoint : ClassTag]: EndpointProps =
    new EndpointProps(implicitly[ClassTag[T]].runtimeClass){
      override def boot = { implicit ctx ⇒
        clazz.getConstructors()(0).newInstance(ctx).asInstanceOf[KrakkenEndpoint]
      }
    }

}
