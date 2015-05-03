package krakken.http

import akka.actor._
import akka.util.Timeout
import krakken.config.GlobalKrakkenConfig
import spray.routing.{HttpService, Route}

trait HttpHandler { this: HttpConfig with Actor ⇒

  val endpointProps: List[EndpointProps]

//  val authenticationProvider: AuthenticationProvider

}

/**
 * Will throw DeathPactException when remote actors terminate
 */
class DefaultHttpHandler(val endpointProps: List[EndpointProps])
  extends HttpHandler with HttpService with Actor with DefaultHttpConfig {

  import context.dispatcher

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    context.system.log.debug("Created new fresh http handler instance. Reason {}", reason)
  }

  implicit val t: Timeout = GlobalKrakkenConfig.ACTOR_TIMEOUT

  override implicit def actorRefFactory: ActorRefFactory = context.system

  val h = endpointProps.head.boot(context.system)
  var remoteActors: Seq[ActorSelection] = Seq.empty[ActorSelection] ++ h.remoteActors

  val routes = endpointProps.tail.foldLeft(h.$route) {
    (chain, next) ⇒
      val booted = next.boot(context.system)
      remoteActors ++= booted.remoteActors
      chain ~ booted.$route
  }

  remoteActors.foreach(_.resolveOne().map(context.watch).onFailure{
    case e: Exception ⇒
      val msg = s"THERE IS NO CONNECTIVITY BETWEEN GATEWAY AND REMOTE ACTOR SYSTEMS: $e"
      throw new Exception(msg, e)
  })

  def receive: Receive =
    runRoute(routes)(exceptionHandler, rejectionHandler,
      context, routingSettings, loggingContext)
}

object DefaultHttpHandler {

  def props(endpointProps: List[EndpointProps]): Props =
    Props(classOf[DefaultHttpHandler], endpointProps)
}
