package krakken

import akka.actor.Props
import akka.event.LoggingAdapter
import akka.io.IO
import krakken.http.{DefaultHttpHandler, EndpointProps, SystemOverseer}
import krakken.system.BootedSystem
import spray.can.Http

import scala.util.Try

class MicroService(val name: String,
                   host: Option[String],
                   port: Option[Int],
                   actorsProps: List[Props],
                   endpointProps: List[EndpointProps],
                   httpHandlerProps: Option[Props])
  extends BootedSystem {

  implicit val log: LoggingAdapter = system.log

  def initActorSystem(remotePort: Option[Int]): Unit = {
    actorsProps.foreach( props ⇒ system.actorOf(props, props.actorClass().getSimpleName))
    log.info(s"$name actor system is up and running")
  }

  def initHttpServer(p: Int, h:String, handler: Props): Unit = {
    val httpHandler = system.actorOf(handler, "http")
    IO(Http) ! Http.Bind(httpHandler, h, port = p)
    log.info(s"$name http interface is listening on $h:$p")
  }

  /* Check valid configuration */
  (host, port, endpointProps, httpHandlerProps) match {
    case (Some(h), Some(p), list, Some(handler)) if list.nonEmpty ⇒
      initActorSystem(port)
      initHttpServer(p, h, handler)
    case (None, None, list, None) if list.isEmpty ⇒
      val remotePort = Try(system.settings.config.getInt("akka.remote.netty.tcp.port")).toOption
      initActorSystem(remotePort)
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
      Some(SystemOverseer.props(DefaultHttpHandler.props(endpointProps))))

  /**
   * Boot Microservice only with actor system
   */
  def apply(name: String, actorProps: List[Props]): MicroService =
    new MicroService(name, None, None, actorProps, List.empty, None)
}