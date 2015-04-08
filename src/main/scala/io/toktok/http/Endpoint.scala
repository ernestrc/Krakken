package io.toktok.http

import akka.actor.{ActorSelection, ActorSystem}
import akka.util.Timeout
import spray.httpx.SprayJsonSupport
import spray.routing._

/**
 * Http endpoint interface
 */
trait Endpoint extends Directives with SprayJsonSupport with AuthenticationDirectives {

  val system: ActorSystem
  val guardianActorPath: String
  val clazz = this.getClass.getCanonicalName

  implicit val timeout: Timeout
  val fallbackTimeout: Timeout

  def route: Route

  def entityActor(entityId: String): ActorSelection =
    system.actorSelection(system / guardianActorPath / entityId.toString)

}
