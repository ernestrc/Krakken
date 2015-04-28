package krakken.http

import akka.actor.{Actor, ActorRefFactory, ActorSelection, Props}
import akka.util.Timeout
import krakken.config.GlobalKrakkenConfig
import krakken.model.EndpointProps
import spray.routing.{HttpService, Route}

trait HttpHandler { this: HttpConfig with Actor ⇒

  val endpointProps: List[EndpointProps]

//  val authenticationProvider: AuthenticationProvider

}

class DefaultHttpHandler(val endpointProps: List[EndpointProps])
  extends HttpHandler with HttpService with Actor with DefaultHttpConfig {

  import context.dispatcher

  implicit val t: Timeout = GlobalKrakkenConfig.ACTOR_TIMEOUT

  override implicit def actorRefFactory: ActorRefFactory = context.system

//  val authenticationProvider: AuthenticationProvider = new TokenAuthentication

  val h = endpointProps.head.boot(context.system)
  var remoteActors: Seq[ActorSelection] = Seq.empty[ActorSelection] ++ h.remoteActors

  val routes: Route = /*authenticationProvider.actionContext { ctx ⇒*/
    endpointProps.tail.foldLeft(h.$route) {
      (chain, next) ⇒
        val booted = next.boot(context.system)
        remoteActors +: booted.remoteActors
        chain ~ booted.$route
    }

  def checkConnectivity() = remoteActors.foreach(_.resolveOne().onFailure {
    case e: Exception ⇒
      val msg = s"THERE IS NO CONNECTIVITY BETWEEN GATEWAY AND REMOTE ACTOR SYSTEMS: $e"
      context.system.log.error(msg)
      throw new Exception(msg)
  })

  def receive: Receive =
    runRoute(routes)(exceptionHandler, rejectionHandler,
      context, routingSettings, loggingContext)
}

object DefaultHttpHandler {

  def props(endpointProps: List[EndpointProps]): Props =
    Props(classOf[DefaultHttpHandler], endpointProps)
}
