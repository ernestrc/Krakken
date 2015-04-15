package krakken.http

import akka.actor.{ActorSelection, ActorSystem}
import akka.event.LoggingAdapter
import akka.util.Timeout
import krakken.model.{Receipt, SID}
import krakken.utils.Implicits._
import spray.httpx.SprayJsonSupport
import spray.json.BasicFormats
import spray.routing._

/**
 * Http endpoint interface
 */
trait Endpoint extends Directives with SprayJsonSupport with AuthenticationDirectives with BasicFormats{

  val system: ActorSystem
  val log: LoggingAdapter = system.log
  val remoteCommandLoc: String
  val remoteQueryLoc: String
  val remoteCommandGuardianPath: String
  val remoteQueryGuardianPath: String
  def commandGuardianActorSelection: ActorSelection = system.actorSelection(remoteCommandLoc / remoteCommandGuardianPath)
  def queryGuardianActorSelection: ActorSelection = system.actorSelection(remoteQueryLoc / remoteQueryGuardianPath)
//  def receiptMarshaller[T : Manifest] = graterMarshallerConverter(Receipt.receiptGrater[T])
//  implicit val receiptOfUnitGrater = receiptMarshaller[Unit]
//  implicit val receiptOfSIDGrater = receiptMarshaller[SID]
  implicit val receiptOfSIDMarshaller = Receipt.receiptFormat[SID]
  implicit val receiptOfUnitMarshaller = Receipt.receiptFormat[Unit]

  implicit val timeout: Timeout
  val fallbackTimeout: Timeout

  private [krakken] def __route: Route = {
    import system.dispatcher
    /* Check connectivity */
    (commandGuardianActorSelection :: queryGuardianActorSelection :: Nil).foreach(_.resolveOne(timeout.duration).onFailure{
      case e:Exception ⇒
        log.error(e, "THERE IS NO CONNECTIVITY BETWEEN REMOTE ACTOR SYSTEM " +
          "AND GATEWAY. COWARDLY SHUTTING DOWN NOW")
        system.shutdown()
    })
    route(commandGuardianActorSelection, queryGuardianActorSelection)
  }

  val route: (ActorSelection, ActorSelection) ⇒ Route

  def entityCommandActor(entityId: String): ActorSelection =
    system.actorSelection(remoteCommandLoc / remoteCommandGuardianPath / entityId.toString)

  def entityQueryActor(entityId: String): ActorSelection =
    system.actorSelection(remoteQueryLoc / remoteQueryGuardianPath / entityId.toString)

}
