package io.toktok.model

import akka.actor.Props
import akka.event.LoggingAdapter
import akka.io.IO
import io.toktok.http.DefaultHttpHandler
import io.toktok.service.BootedSystem
import spray.can.Http

class MicroService(name: String,
                   host: String,
                   port: Int,
                   actorsProps: List[Props],
                   endpointsProps: List[EndpointProps],
                   httpHandlerProps: Props)
  extends BootedSystem {

  implicit val log: LoggingAdapter = system.log

  actorsProps.foreach( props ⇒ system.actorOf(props, props.actorClass().getSimpleName))

  val httpHandler = system.actorOf(httpHandlerProps)

  IO(Http) ! Http.Bind(httpHandler, host, port = port)

  log.info(s"$name microservice booted up in host $host and port $port")

}

object MicroService{

  def apply(name:String, host:String, port:Int, actorProps: List[Props],
            endpointProps: List[EndpointProps]): MicroService =
    new MicroService(name, host, port, actorProps, endpointProps,
      DefaultHttpHandler.props(endpointProps))
}