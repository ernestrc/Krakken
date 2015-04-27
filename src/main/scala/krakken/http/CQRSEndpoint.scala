package krakken.http

import akka.actor.{ActorSelection, ActorSystem}
import akka.event.LoggingAdapter
import akka.util.Timeout
import krakken.model.{Query, Command, Receipt, SID}
import krakken.utils.Implicits._
import spray.http.HttpHeaders._
import spray.httpx.SprayJsonSupport
import spray.http.ContentTypes._
import spray.json.{DefaultJsonProtocol, RootJsonFormat, BasicFormats}
import spray.routing._

import scala.concurrent.{ExecutionContext, Future}

/**
 * Http endpoint interface
 */
trait KrakkenEndpoint extends Directives
with SprayJsonSupport with AuthenticationDirectives with DefaultJsonProtocol {

  val system: ActorSystem

  def $route: Route
}

trait Endpoint extends KrakkenEndpoint {

  val system: ActorSystem

  def $route: Route = route

  def route: Route
}

trait CQRSEndpoint extends KrakkenEndpoint {

  val system: ActorSystem
  implicit val log: LoggingAdapter = system.log
  val remoteCommandLoc: String
  val remoteQueryLoc: String
  val remoteCommandGuardianPath: String
  val remoteQueryGuardianPath: String

  def commandGuardianActorSelection: ActorSelection = system.actorSelection(remoteCommandLoc / remoteCommandGuardianPath)

  def queryGuardianActorSelection: ActorSelection = system.actorSelection(remoteQueryLoc / remoteQueryGuardianPath)

  implicit val timeout: Timeout
  val fallbackTimeout: Timeout

  def $route: Route = { ctx: RequestContext ⇒
    log.debug("{} received request {}", this.getClass.getSimpleName, ctx.request)
    import system.dispatcher
    /* Check connectivity */
    (commandGuardianActorSelection :: queryGuardianActorSelection :: Nil).foreach(_.resolveOne(timeout.duration).onFailure {
      case e: Exception ⇒
        log.warning("THERE IS NO CONNECTIVITY BETWEEN GATEWAY AND REMOTE ACTOR SYSTEMS: {}", e)
    })
    route(commandGuardianActorSelection, queryGuardianActorSelection)(ctx)
  }

  val route: (ActorSelection, ActorSelection) ⇒ Route

  def entityCommandActor(entityId: String): ActorSelection =
    system.actorSelection(remoteCommandLoc / remoteCommandGuardianPath / entityId.toString)


  def entityQueryActor(entityId: String): ActorSelection =
    system.actorSelection(remoteQueryLoc / remoteQueryGuardianPath / entityId.toString)

  implicit class pimpedSelection(selection: ActorSelection) {

    import akka.pattern.ask

    def ??(cmd: Command)(implicit ctx: ExecutionContext): Future[Any] = {
      selection.ask(cmd)(fallbackTimeout).recoverWith {
        case exception: Exception ⇒
          log.warning(s"Worker of ${cmd.entityId} is not responding!")
          commandGuardianActorSelection.ask(cmd)
      }
    }

    def ??(query: Query)(implicit ctx: ExecutionContext): Future[Any] = {
      selection.ask(query)(fallbackTimeout).recoverWith {
        case exception: Exception ⇒
          log.warning(s"Query Worker is not responding!")
          commandGuardianActorSelection.ask(query)
      }
    }
  }

}
