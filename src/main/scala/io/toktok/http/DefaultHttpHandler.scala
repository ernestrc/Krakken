package io.toktok.http

import akka.actor.{Actor, ActorRefFactory, Props}
import io.toktok.model.EndpointProps
import spray.routing.{HttpService, Route}

trait HttpHandler { this: HttpConfig with Actor ⇒

  val endpointProps: List[EndpointProps]

}

class DefaultHttpHandler(val endpointProps: List[EndpointProps])
  extends HttpHandler with HttpService with Actor with DefaultHttpConfig {

  override implicit def actorRefFactory: ActorRefFactory = context.system

  val routes: Route = {
    var _routes: Route = null
    endpointProps.foreach { endpoint =>
      val route = endpoint.boot(context.system).route
      _routes = {
        if (_routes != null) _routes ~ route
        else route
      }
    }
    _routes
  }

  def receive: Receive =
    runRoute(routes)(exceptionHandler, rejectionHandler,
      context, routingSettings, loggingContext)
}

object DefaultHttpHandler {

  def props(endpointProps: List[EndpointProps]): Props =
    Props(classOf[DefaultHttpHandler], endpointProps)
}
