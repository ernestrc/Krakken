package krakken

import akka.actor.Props
import akka.event.LoggingAdapter
import akka.io.IO
import krakken.config.GlobalConfig
import krakken.http.DefaultHttpHandler
import krakken.model.EndpointProps
import krakken.system.BootedSystem
import spray.can.Http

class MicroService(val name: String,
                   host: Option[String],
                   port: Option[Int],
                   actorsProps: List[Props],
                   endpointProps: List[EndpointProps],
                   httpHandlerProps: Option[Props])
  extends BootedSystem {

  implicit val log: LoggingAdapter = system.log

  def initActorSystem(): Unit = {
    actorsProps.foreach( props ⇒ system.actorOf(props, props.actorClass().getSimpleName))
    log.info(s"$name actor system is listening on ${GlobalConfig.akkaRemoteHost}:${GlobalConfig.akkaRemotePort}")
  }

  def initHttpServer(p: Int, h:String, handler: Props): Unit = {
    val httpHandler = system.actorOf(handler)
    IO(Http) ! Http.Bind(httpHandler, h, port = p)
    log.info(s"$name http interface is listening on $h:$p")
  }

  /* Check valid configuration */
  (host, port, endpointProps, httpHandlerProps) match {
    case (Some(h), Some(p), list, Some(handler)) if list.nonEmpty ⇒
      initActorSystem()
      initHttpServer(p, h, handler)
    case (None, None, list, None) if list.isEmpty ⇒
      initActorSystem()
    case anyElse ⇒
      throw new Exception(s"Combination $host - $port - $endpointProps - $httpHandlerProps not valid!")
  }
}

object MicroService{

  /**
   * Boot Microservice with actor system and http server
   */
  def apply(name:String, host:String, port:Int, actorProps: List[Props],
            endpointProps: List[EndpointProps]): MicroService =
    new MicroService(name, Some(host), Some(port), actorProps, endpointProps,
      Some(DefaultHttpHandler.props(endpointProps)))

  /**
   * Boot Microservice only with actor system
   */
  def apply(name: String, actorProps: List[Props]): MicroService =
    new MicroService(name, None, None, actorProps, List.empty, None)
}