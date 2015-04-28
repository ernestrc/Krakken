package krakken.system

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.event.LoggingAdapter
import com.mongodb.casbah.MongoClient
import krakken.config.GlobalKrakkenConfig
import krakken.dal.MongoSource
import krakken.model.Exceptions.KrakkenException
import krakken.model._
import krakken.utils.Implicits._
import krakken.utils.io._

import scala.reflect.ClassTag

/**
 * Created by ernest on 4/2/15.
 */
abstract class EventSourcedCommandActor[T <: Event : ClassTag : FromHintGrater] extends Actor with ActorLogging {


  override def postStop(): Unit = {
    subscriptions.foreach(_.unsubscribe())
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    subscriptions.foreach(_.subscribe)
  }

  override def preStart(): Unit = {
    log.info(s"Booting up event sourced actor - ${self.path.name}...")
    val count: Int = entityId.map{ id ⇒
      source.findAllByEntityId(id).foldLeft(0) { (cc, ev) ⇒ eventProcessor(ev); cc + 1}
    }.getOrElse{
      source.listAll.foldLeft(0) { (cc, ev) ⇒ eventProcessor(ev); cc + 1}
    }
    log.info(s"Finished booting up event sourced actor - ${self.path.name}. Applied $count events")
    subscriptions.foreach(_.subscribe())
  }

  val name: String = self.path.name

  implicit val subscriber: ActorRef = self

  implicit val entityId: Option[SID]

  implicit val logger: LoggingAdapter = log

  val mongoContainer = getContainerLink(GlobalKrakkenConfig.dataContainer)
  val mongoHost: String =  mongoContainer.map(_.host.ip).getOrElse(GlobalKrakkenConfig.mongoHost)
  val mongoPort: Int = mongoContainer.map(_.port).getOrElse(GlobalKrakkenConfig.mongoPort)
  val dbName: String = GlobalKrakkenConfig.dbName
  log.debug("{} container linked -> {}", GlobalKrakkenConfig.dataContainer, mongoContainer)
  val db = MongoClient(mongoHost, mongoPort)(dbName)
  val source = new MongoSource[T](db)

  val subscriptions: List[Subscription]

  val eventProcessor: PartialFunction[Event, Unit]

  val commandProcessor: PartialFunction[Command, List[T]]

  def receive: Receive = {
    case cmd: Command ⇒
      log.debug("{} received command {}", self.path.name, cmd)
      val receipt: Receipt[_] = try {
        val events = commandProcessor(cmd)
        events.foreach(source.save)
        events.foreach(eventProcessor)
        Receipt(success=true, entity=events.last, message = "OK") Ω { receipt ⇒
          s"Successfully processed command $cmd and generated ${receipt.entity}"
        }
      } catch {
        case ex: KrakkenException ⇒
          log.debug(s"Contingency: $ex")
          Receipt.error(ex)
        case err: Exception ⇒
          log.error(err, s"There was an error in CommandSide actor when processing command $cmd!")
          Receipt.error(err)
      }
      sender() ! receipt
      log.debug(s"Actor $name processed $cmd")
    case anyElse ⇒ log.error(s"Oops, it looks like I shouldn't have received $anyElse")
  }
}
