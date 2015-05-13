package krakken.http

import akka.actor.SupervisorStrategy.{Stop, Restart, Escalate}
import akka.actor._
import akka.io.IO
import akka.util.Timeout
import krakken.config.GlobalKrakkenConfig
import spray.can.Http
import spray.routing.HttpService

trait HttpHandler {
  this: HttpConfig with Actor ⇒

  val endpointProps: List[EndpointProps]

  //  val authenticationProvider: AuthenticationProvider

}

/**
 * Will throw DeathPactException when remote actors terminate
 */
class DefaultHttpHandler(val endpointProps: List[EndpointProps])
  extends HttpHandler with HttpService with Actor with DefaultHttpConfig {

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy.apply(){
    case e: DeathPactException ⇒ Escalate
    case e: ActorNotFound ⇒ Escalate
    case e: Exception ⇒ Restart
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    context.system.log.debug("Handler up and running in {}", self.path)
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    context.system.log.warning("Created new fresh http handler instance. Reason {}", reason)
    preStart()
  }

  implicit val t: Timeout = GlobalKrakkenConfig.ACTOR_TIMEOUT

  override implicit def actorRefFactory: ActorRefFactory = context.system

  val h = endpointProps.head.boot(context)
  var remoteActors: List[ActorRef] = h.remoteActors

  val routes = endpointProps.tail.foldLeft(h.$route) {
    (chain, next) ⇒
      val booted = next.boot(context)
      remoteActors ++= booted.remoteActors
      chain ~ booted.$route
  }

  remoteActors.foreach(context.watch)

  def receive: Receive =
    runRoute(routes)(exceptionHandler, rejectionHandler,
      context, routingSettings, loggingContext)
}

object DefaultHttpHandler {

  def props(endpointProps: List[EndpointProps]): Props =
    Props(classOf[DefaultHttpHandler], endpointProps)
}
