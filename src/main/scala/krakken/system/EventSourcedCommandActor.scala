package krakken.system

import akka.actor.{Actor, ActorLogging, ActorRef}
import krakken.dal.MongoSource
import krakken.model.Exceptions.KrakkenException
import krakken.model._

import scala.reflect.ClassTag

/**
 * Created by ernest on 4/2/15.
 */
abstract class EventSourcedCommandActor[T <: Event : ClassTag] extends Actor with ActorLogging {

  override def postStop(): Unit = {
    subscriptions.foreach(_.unsubscribe())
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    subscriptions.foreach(_.unsubscribe())
  }

  override def preStart(): Unit = {
    log.info(s"Booting up event sourced actor - ${self.path.name}...")
    val count: Int = entityId.map{ id ⇒
      source.findAllByEntityId(id).foldLeft(0) { (cc, ev) ⇒ eventProcessor(ev); cc + 1}
    }.getOrElse{
      source.listAll.foldLeft(0) { (cc, ev) ⇒ eventProcessor(ev); cc + 1}
    }
    log.info(s"Finished booting up event sourced actor - ${self.path.name}. Applied $count events")
  }

  val name: String = self.path.name

  implicit val subscriber: ActorRef = self

  implicit val entityId: Option[SID]

  val subscriptions: List[Subscription]

  val source: MongoSource[T]

  val eventProcessor: PartialFunction[Event, Unit]

  val commandProcessor: PartialFunction[Command, List[T]]

  def receive: Receive = {
    case cmd: Command ⇒
      val receipt: Receipt = try {
        val events = commandProcessor(cmd)
        events.foreach(source.save)
        events.foreach(eventProcessor)
        Receipt(success = true, updated = entityId.getOrElse("*"), message = "OK")
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
