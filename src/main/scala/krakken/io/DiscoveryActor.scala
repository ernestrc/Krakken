package krakken.io

import akka.actor._
import akka.pattern._
import com.novus.salat._
import com.novus.salat.global._
import krakken.config.GlobalKrakkenConfig
import krakken.io.DiscoveryActor.{Exhausted, Find}
import spray.client.pipelining._
import spray.http._
import spray.httpx.unmarshalling._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

/**
 * Service discovery actor that can be used as a helper to find other services via etcd, docker linked containers,
 * or simply plain configuration. It can (and should) be watched for dynamic reconfiguration the system.
 * If service was discovered via etcd, this actor will install a watching mechanism and will throw DeathPactException
 * if the key was changed.
 *
 * If planned to use via etcd, the microservice container/host should have an ETCD_ENDPOINT env variable
 * that can be obtained in host like this:
 *
 * ETCD_ENDPOINT="$(ifconfig docker0 | awk '/\<inet\>/ { print $2}'):4001"
 */
class DiscoveryActor extends Actor with ActorLogging {

  import context.dispatcher

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = false, maxNrOfRetries = -1) {
    case e: DeathPactException ⇒ akka.actor.SupervisorStrategy.Escalate
    case e: Exception ⇒ akka.actor.SupervisorStrategy.Restart
  }

  var etcd: Option[Service] = None

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    val endpoint = System.getenv(GlobalKrakkenConfig.ETCD)
    if (endpoint != null) {
      log.debug(s"${GlobalKrakkenConfig.ETCD} env variable equals $endpoint")
      val s = endpoint.split(":")
      val service = Service(Host("etcd", s(0)), s(1).toInt)
      val uri = service.getPathUri("/version")
      log.debug("Checking access to etcd in {} ...", uri)
      val verified: Option[Service] = Try {
        val f = pipeline(Get(uri)).map {
          case response if response.status.isSuccess ⇒
            service
          case response ⇒
            val msg = s"Request to etcd wat not successful: $response"
            throw new Exception(msg)
        }
        Await.result(f, GlobalKrakkenConfig.ACTOR_TIMEOUT)
      }.recover {
        case e: Exception ⇒
          log.error(e, "Bummer!")
          throw e
      }.toOption
      etcd = verified
    } else log.warning(s"Could not find ${GlobalKrakkenConfig.ETCD} (etcd) environment variable")
    log.info(s"Discovery actor finished booting up. ETCD instance -> $etcd")
  }

  val pipeline: HttpRequest ⇒ Future[HttpResponse] =
    sendReceive ~> unmarshal[HttpResponse]

  def find(serviceName: String): Future[Service] =
    findInEtcd(serviceName)
      .recoverWith {
      case _: Exception ⇒ Future(getContainerLink(serviceName).get).andThen {
        case Failure(e) ⇒ log.warning(s"Could not find docker container linked ($serviceName)")
        case Success(ss) ⇒ log.info(s"Found service $serviceName as a linked docker container")
      }
    }

  def watch(s: Service): Unit = {
    val watcher = context.actorOf(Props(classOf[WatchActor], etcd.get, s))
    context.watch(watcher)
    watcher ! "start"
    context.system.scheduler.scheduleOnce(DiscoveryActor.etcdPollingDuration, self, Exhausted(s, watcher))
  }

  def findInEtcd(key: String): Future[Service] =
    Future(etcd.get).flatMap { endpoint ⇒
      val uri = endpoint.getPathUri(DiscoveryActor.SERVICE_KEYS + key)
      log.debug("Querying etcd -> {} ...", uri)
      pipeline(Get(uri)).map { response ⇒
        log.debug("Completed request to etcd successfully")
        response.entity.as[EtcdResponse] match {
          case Right(s: EtcdResponse) ⇒
            val spl = s.node.value.split(":")
            Service(Host(key, spl(0)), spl(1).toInt)
        }
      }.andThen {
        case Failure(e) ⇒ log.warning(s"There was a problem when querying etcd: $e")
      }
    }.andThen {
      case Failure(e) ⇒ log.warning(s"Could not find service $key in etcd. Reason: $e")
      case Success(s) ⇒
        log.info(s"Found service $key in etcd: $s. Starting watch...")
      //        watch(s) FIXME temporarily deactivated
    }

//  def $register

//  def register(name: String, port: Option[Int]) = {
//    (port, etcd, krakken.io.loadHosts().find(_.alias == name)) match {
//      case (Some(p), Some(s), Some(host)) ⇒
//        val uri = s.getPathUri(DiscoveryActor.SERVICE_KEYS + name)
//        pipeline(Post(uri, s"""{ "value" : "${host.ip}:$p" , "ttl": "${GlobalKrakkenConfig.REGISTRATION_TTL}"}""")).map {
//          case response if response.status.isSuccess ⇒ log.info("Completed registration to etcd successfully")
//        }.andThen{
//          case Failure(e) ⇒ log.error(s"Could not register microservice to etcd. Reason $e")
//        }
//      case _ ⇒ log.warning(s"Could not register microservice with name '$name' in etcd service '$etcd' with main communications port '$port'")
//    }
//  }

  override def receive: Actor.Receive = {
//    case Register(name, port) ⇒ register(name, port)
//    case UpdateRegistration(name, port) ⇒
    case Find(name) ⇒
      log.info(s"Received request to find service with name $name")
      find(name) pipeTo sender()
    case Exhausted(serviceToWatch, watcher) ⇒
      context.unwatch(watcher)
      watch(serviceToWatch)
      watcher ! PoisonPill
  }
}

object DiscoveryActor {

  val SERVICE_KEYS = "/v2/keys/service/"

//  case class Register(name: String, port: Option[Int])
//
//  case class UpdateRegistration(name: String, port: Option[Int])

  case class Find(service: String)

  case class Exhausted(serviceToWatch: Service, watcher: ActorRef)

  val etcdPollingDuration = GlobalKrakkenConfig.ETCD_POLLING

}

class WatchActor(etcd: Service, toWatch: Service) extends Actor with ActorLogging {

  import context.dispatcher

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug(s"Starting watch over etcd key /services/${toWatch.host.alias}")
  }


  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.debug("I'm Exhausted ({})!", self.path.name)
  }

  val pipeline: HttpRequest ⇒ Future[HttpResponse] =
    sendReceive(context.system, context.dispatcher, 5.days) ~> unmarshal[HttpResponse]

  def watch(serviceName: String) = {
    val uri = etcd.getPathUri(DiscoveryActor.SERVICE_KEYS + serviceName).withQuery("wait" → "true")
    log.debug("Polling etcd for changes in -> {} ...", uri)
    pipeline(Get(uri)).map { response ⇒
      log.warning(s"etcd key /services/$serviceName changed!")
      self ! PoisonPill
    }
  }

  override def receive: Actor.Receive = {
    case "start" ⇒ watch(toWatch.host.alias)
  }

}

/**
 * https://coreos.com/etcd/docs/0.4.7/etcd-api/
 *
 * { "action": "set",
 * "node": {
 * "createdIndex": 2,
 * "key": "/message",
 * "modifiedIndex": 2,
 * "value": "Hello world"
 * }
 * }
 */
case class EtcdResponse(action: String, node: EtcdNode)

case class EtcdNode(createdIndex: Int, key: String, modifiedIndex: Int, value: String)

object EtcdNode {

  import krakken.utils.Implicits._

  implicit val etcdActionJsonFormat: Unmarshaller[EtcdNode] = grater[EtcdNode]
}

object EtcdResponse {

  import krakken.utils.Implicits._

  implicit val etcdActionJsonFormat: Unmarshaller[EtcdResponse] = grater[EtcdResponse]
}