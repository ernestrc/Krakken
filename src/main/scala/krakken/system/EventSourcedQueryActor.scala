package krakken.system

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.event.LoggingAdapter
import krakken.dal.MongoSource
import krakken.model._

import scala.reflect.ClassTag
import scala.util.Try

/**
 * Created by ernest on 4/12/15.
 */
abstract class EventSourcedQueryActor[T <: Event : ClassTag] extends Actor with ActorLogging {

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

  implicit val loggger: LoggingAdapter = log

  implicit val entityId: Option[SID]

  val subscriptions: List[Subscription]

  val source: MongoSource[T]

  val eventProcessor: PartialFunction[Event, Unit]

  val queryProcessor: PartialFunction[Query, View]

  def receive:Receive = {
    case e: Event ⇒
      val receipt: Receipt[_] = try {
        source.save(e.asInstanceOf[T])
        Receipt(success = true, entity = Some(eventProcessor(e)), message = "OK")
      } catch {
        case ex: Exception ⇒
          log.error(s"There was an error in QuerySide when processing $e")
          Receipt.error(ex)
      }
      sender() ! receipt
    case q: Query if queryProcessor.isDefinedAt(q) ⇒
      val receipt: Receipt[_] = Try{
        queryProcessor(q)
      }.map{
        view ⇒ Receipt(success=true, entity=Some(view))
      }.recover{
        case e:Exception ⇒ Receipt.error(e)
      }.get
      sender() ! receipt
  }

}
