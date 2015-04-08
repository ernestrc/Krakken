package io.toktok.model

import akka.actor.ActorSystem
import io.toktok.http.Endpoint

import scala.reflect.ClassTag

abstract class EndpointProps(val clazz: Class[_], val args: Any*) {

  def boot: (ActorSystem) ⇒ Endpoint

}

object EndpointProps{

  def apply[T <: Endpoint : ClassTag](args: Any*): EndpointProps =
    new EndpointProps(implicitly[ClassTag[T]].runtimeClass){
      override def boot = { implicit ctx ⇒
        clazz.getConstructors()(0).newInstance(ctx, args).asInstanceOf[Endpoint]
      }
    }

  def apply[T <: Endpoint : ClassTag]: EndpointProps =
    new EndpointProps(implicitly[ClassTag[T]].runtimeClass){
      override def boot = { implicit ctx ⇒
        clazz.getConstructors()(0).newInstance(ctx).asInstanceOf[Endpoint]
      }
    }

}
