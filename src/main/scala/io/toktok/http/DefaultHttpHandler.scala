package io.toktok.http

import akka.actor.{Actor, ActorRefFactory, Props}
import io.toktok.model.EndpointProps
import spray.routing.{HttpService, Route}

trait HttpHandler { this: HttpConfig with Actor ⇒

  val endpointProps: List[EndpointProps]

//  val authenticationProvider: AuthenticationProvider

}

class DefaultHttpHandler(val endpointProps: List[EndpointProps])
  extends HttpHandler with HttpService with Actor with DefaultHttpConfig {

  override implicit def actorRefFactory: ActorRefFactory = context.system

//  val authenticationProvider: AuthenticationProvider = new TokenAuthentication

  val routes: Route = /*authenticationProvider.actionContext { ctx ⇒*/
    endpointProps.tail.foldLeft(endpointProps.head.boot(context.system).__route) {
      (chain, next) ⇒ chain ~ next.boot(context.system).__route
    }
  //}


  def receive: Receive =
    runRoute(routes)(exceptionHandler, rejectionHandler,
      context, routingSettings, loggingContext)
}

object DefaultHttpHandler {

  def props(endpointProps: List[EndpointProps]): Props =
    Props(classOf[DefaultHttpHandler], endpointProps)
}
