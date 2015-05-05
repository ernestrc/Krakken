package krakken.http

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.event.LoggingAdapter
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import krakken.io.{DiscoveryActor, Service}
import krakken.model.{Command, Query}
import krakken.utils.Implicits._
import spray.httpx.SprayJsonSupport
import spray.json.DefaultJsonProtocol
import spray.routing._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

/**
 * Http endpoint interface
 */
trait KrakkenEndpoint extends Directives
with SprayJsonSupport with AuthenticationDirectives with DefaultJsonProtocol {

  val context: ActorContext

  val remoteActors : List[ActorRef]

  def $route: Route
}

trait Endpoint extends KrakkenEndpoint {

  val context: ActorContext

  val remoteActors : List[ActorRef] = List.empty[ActorRef]

  def $route: Route = route

  def route: Route
}

abstract class GatewayEndpoint extends KrakkenEndpoint {
  
  val bootTimeout: FiniteDuration = FiniteDuration.apply(5, TimeUnit.SECONDS)

  val context: ActorContext

  import context.dispatcher

  implicit val log: LoggingAdapter = context.system.log
  val commandService: String
  val queryService: String
  val remoteCommandGuardianPath: String
  val remoteQueryGuardianPath: String

  val discoveryActor = context.actorOf(Props(classOf[DiscoveryActor]))

  lazy val linkedCommandServiceUrl: Option[String] = Try(Await.result(discoveryActor.ask(DiscoveryActor.Find(commandService))
    .mapTo[Service]
    .map(_.toAkkaUrl), bootTimeout))
    .toOption
  lazy val commandServiceUrl: String = linkedCommandServiceUrl.getOrElse(ConfigFactory.load().getString(s"krakken.services.$commandService"))
  lazy val linkedQueryServiceUrl: Option[String] = Try(Await.result(discoveryActor.ask(DiscoveryActor.Find(queryService))
    .mapTo[Service]
    .map(_.toAkkaUrl), bootTimeout))
    .toOption
  lazy val queryServiceUrl: String = linkedQueryServiceUrl.getOrElse(ConfigFactory.load().getString(s"krakken.services.$queryService"))

  def commandGuardianActorSelection: ActorSelection = context.actorSelection(commandServiceUrl / remoteCommandGuardianPath)

  def queryGuardianActorSelection: ActorSelection = context.actorSelection(queryServiceUrl / remoteQueryGuardianPath)
  
  lazy val commandGuardianActor: ActorRef = Await.result(commandGuardianActorSelection.resolveOne(bootTimeout), bootTimeout)
  lazy val queryGuardianActor: ActorRef = Await.result(queryGuardianActorSelection.resolveOne(bootTimeout), bootTimeout)

  lazy val remoteActors : List[ActorRef] = commandGuardianActor :: queryGuardianActor :: Nil

  implicit val timeout: Timeout
  val fallbackTimeout: Timeout

  def $route: Route = { ctx: RequestContext ⇒
    log.debug("{} received request {}", this.getClass.getSimpleName, ctx.request)
    context.stop(discoveryActor)
    route(commandGuardianActor, queryGuardianActor)(ctx)
  }

  val route: (ActorRef, ActorRef) ⇒ Route

  def entityCommandActor(entityId: String): ActorSelection =
    context.actorSelection(commandService / remoteCommandGuardianPath / entityId.toString)


  def entityQueryActor(entityId: String): ActorSelection =
    context.actorSelection(queryService / remoteQueryGuardianPath / entityId.toString)

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
