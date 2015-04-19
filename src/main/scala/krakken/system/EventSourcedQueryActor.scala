package krakken.system

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.event.LoggingAdapter
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import krakken.config.GlobalConfig
import krakken.model._

import scala.reflect.ClassTag
import scala.util.Try
import com.mongodb.casbah.Imports._

/**
 * Created by ernest on 4/12/15.
 */
abstract class EventSourcedQueryActor[T <: Event : ClassTag : FromHintGrater] extends Actor with ActorLogging {
  import krakken.utils.Implicits._
  override def postStop(): Unit = {
    subscriptions.foreach(_.unsubscribe())
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    subscriptions.foreach(_.subscribe())
  }

  override def preStart(): Unit = {
    log.info(s"Booting up event sourced QUERY actor - ${self.path.name}...")
    val count: Int =
      $source.foldLeft(0) { (cc, ev) ⇒ eventProcessor(ev.asInstanceOf[Event]); cc + 1}
    log.info(s"Finished booting up event sourced QUERY actor - ${self.path.name}. Applied $count events")
    subscriptions.foreach(_.subscribe())
  }

  val name: String = self.path.name

  val client = MongoClient(GlobalConfig.mongoHost)
  val db = client(GlobalConfig.mongoDb)
  val subs = db(classOf[Subscription].getSimpleName)
  val subscriptionSerializers: FromHintGrater[AnyRef]

  implicit val entityId: Option[SID]

  def $source = subs.find(sourceQuery).toList.map{ mongoObject ⇒
    val hint = mongoObject.as[String]("_typeHint").toHint
    subscriptionSerializers.apply(hint).asObject(mongoObject)
  }
  lazy val sourceQuery: MongoDBObject = {
    val hints = subscriptions.map(s ⇒ s.subscribedTo.getCanonicalName)
    entityId match {
      case Some(id) ⇒
        MongoDBObject("_typeHint" → MongoDBObject("$in" → hints),
          "entityId" → id)
      case _ ⇒ MongoDBObject("_typeHint" → MongoDBObject("$in" → hints))
    }
  }

  implicit val subscriber: ActorRef = self

  implicit val logger: LoggingAdapter = log

  val subscriptions: List[Subscription]

  val eventProcessor: PartialFunction[Event, Unit]

  val queryProcessor: PartialFunction[Query, View]

  def receive:Receive = {
    case e: Event ⇒
      log.debug(s"Received $e event")
      val receipt: Receipt[_] = try {
        Receipt(success = true, entity = Some(eventProcessor(e)), message = "OK")
      } catch {
        case ex: Exception ⇒
          log.error(s"There was an error in QuerySide when processing $e. Reason $ex")
          Receipt.error(ex)
      }
      sender() ! receipt
    case q: Query if queryProcessor.isDefinedAt(q) ⇒
      log.debug(s"Received query $q")
      val receipt: Receipt[_] = Try{
        queryProcessor(q) Ω { v ⇒
          s"Successfully mapped query to view $v"
        }
      }.map{
        view ⇒ Receipt(success=true, entity=view)
      }.recover{
        case e:Exception ⇒ Receipt.error(e)
      }.get
      sender() ! receipt
  }

}
