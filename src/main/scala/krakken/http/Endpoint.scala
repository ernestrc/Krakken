package krakken.http

import akka.actor.{ActorSelection, ActorSystem}
import akka.event.LoggingAdapter
import akka.util.Timeout
import krakken.model.Receipt
import krakken.utils.Implicits._
import spray.httpx.SprayJsonSupport
import spray.routing._

/**
 * Http endpoint interface
 */
trait Endpoint extends Directives with SprayJsonSupport with AuthenticationDirectives {

  val system: ActorSystem
  val log: LoggingAdapter = system.log
  val remoteCommandLoc: String
  val remoteQueryLoc: String
  val remoteCommandGuardianPath: String
  val remoteQueryGuardianPath: String
  def commandGuardianActorSelection: ActorSelection = system.actorSelection(remoteCommandLoc / remoteCommandGuardianPath)
  def queryGuardianActorSelection: ActorSelection = system.actorSelection(remoteQueryLoc / remoteQueryGuardianPath)
  implicit val receiptGrater = graterMarshallerConverter(Receipt.receiptGrater)

  implicit val timeout: Timeout
  val fallbackTimeout: Timeout

  private [krakken] def __route: Route = {
    import system.dispatcher
    /* Check connectivity */
    commandGuardianActorSelection.resolveOne(timeout.duration).onFailure{
      case e:Exception ⇒
        log.error(e, "THERE IS NO CONNECTIVITY BETWEEN REMOTE ACTOR SYSTEM " +
          "AND GATEWAY. COWARDLY SHUTTING DOWN NOW")
        system.shutdown()
    }
    route(commandGuardianActorSelection)
  }

  val route: (ActorSelection) ⇒ Route

  def entityCommandActor(entityId: String): ActorSelection =
    system.actorSelection(remoteCommandLoc / remoteCommandGuardianPath / entityId.toString)

  def entityQueryActor(entityId: String): ActorSelection =
    system.actorSelection(remoteQueryLoc / remoteQueryGuardianPath / entityId.toString)

}
