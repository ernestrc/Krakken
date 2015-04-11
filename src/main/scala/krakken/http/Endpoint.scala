package krakken.http

import akka.actor.{ActorSelection, ActorSystem}
import akka.event.LoggingAdapter
import akka.util.Timeout
import krakken.utils.Implicits._
import spray.httpx.SprayJsonSupport
import spray.routing._

/**
 * Http endpoint interface
 */
trait Endpoint extends Directives with SprayJsonSupport with AuthenticationDirectives {

  val log: LoggingAdapter
  val system: ActorSystem
  val remoteSystemLoc: String
  val remoteGuardianPath: String
  val guardianActorSelection: ActorSelection

  implicit val timeout: Timeout
  val fallbackTimeout: Timeout

  private [krakken] def __route: Route = {
    import system.dispatcher
    /* Check connectivity */
    guardianActorSelection.resolveOne(timeout.duration).onFailure{
      case e:Exception ⇒
        log.error(e, "THERE IS NO CONNECTIVITY BETWEEN REMOTE ACTOR SYSTEM " +
          "AND GATEWAY. COWARDLY SHUTTING DOWN NOW")
        system.shutdown()
    }
    route(guardianActorSelection)
  }

  val route: (ActorSelection) ⇒ Route

  def entityActor(entityId: String): ActorSelection =
    system.actorSelection(remoteSystemLoc / remoteGuardianPath / entityId.toString)

}
