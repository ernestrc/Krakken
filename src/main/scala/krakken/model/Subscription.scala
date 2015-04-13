package krakken.model

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.util.Timeout
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoClient
import com.mongodb.{BasicDBObjectBuilder, Bytes, DBCursor}
import com.novus.salat.Grater
import krakken.config.GlobalConfig
import krakken.model.Exceptions.FailedToConsumeSubscription
import krakken.model.SubscriptionMaster.{CursorEmpty, DispatchedEvent, Subscribe}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

trait Subscription {
  def unsubscribe(): Unit
}

//TODO throw exception when non-replicaSet?
case class AkkaSubscription[A <: Event : ClassTag, B : ClassTag]
(serializer: Grater[A], localDb: MongoDB, remoteSourceHost: String, remoteDbName: String)(translator: A ⇒ B)
(implicit context: ActorContext, subscriber: ActorRef, entityId: Option[String]) extends Subscription {

  val subscribedTo = implicitly[ClassTag[A]].runtimeClass
  val subscribedTypeHint = subscribedTo.getCanonicalName

  val master = context.actorOf(Props(classOf[SubscriptionMaster[A,B]],
    translator, subscribedTypeHint, remoteSourceHost, remoteDbName, serializer, entityId, localDb, subscriber))

  def unsubscribe(): Unit = context.stop(master)

}

object AkkaSubscription{
  def forView[A <: Event : ClassTag](serializer: Grater[A], localDb: MongoDB, remoteSourceHost: String, remoteDbName: String)
    (implicit context: ActorContext, subscriber: ActorRef, entityId: Option[String]): Subscription =
    AkkaSubscription[A,A](serializer, localDb, remoteSourceHost, remoteDbName)(a ⇒ a)
}

object SubscriptionMaster {

  case class Subscribe(cursor: DBCursor)
  case class DispatchedEvent(ts: ObjectId, event: DBObject)
  case object CursorEmpty

}

class SubscriptionMaster[A <: Event, B](
  translator: A ⇒ B,
  typeHint: String,
  remoteSourceHost: String,
  remoteDbName: String,
  serializer: Grater[A],
  entityId: Option[String],
  localDb: MongoDB,
  subscriber: ActorRef
) extends Actor with ActorLogging {

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
    case e: Exception =>
      log.warning(s"Restarting subscription worker. Reason $e")
      worker ! Subscribe(generateCursor(lastTs))
      Restart
  }

  val initQuery: DBObject = entityId match {
    case Some(id) ⇒
      MongoDBObject(
        "_typeHint" → typeHint,
        "entityId" → entityId
      )
    case _ ⇒ MongoDBObject("_typeHint" → typeHint)
  }

  val cursorQuery = { ts: ObjectId ⇒
    entityId match {
      case Some(id) ⇒ MongoDBObject(
        "o._id" → MongoDBObject("$gt" → ts),
        "o._typeHint" → typeHint,
        "o.entityId" → entityId,
        "ns" → MongoDBObject( "$ne" → s"$remoteDbName.subscriptions")
      )
      case _ ⇒ MongoDBObject(
        "o._id" → MongoDBObject("$gt" → ts),
        "o._typeHint" → typeHint,
        "ns" → MongoDBObject( "$ne" → s"$remoteDbName.subscriptions")
      )
    }
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug(s"Subscribing to events in mongodb://$eventSourceHost of type $typeHint")
    lastTs = Try(localColl.underlying.find(initQuery).sort(BasicDBObjectBuilder.start("_id", -1).get())
      .limit(1).one().as[ObjectId]("_id")) match {
      case Success(i) ⇒
        log.debug(s"Found a $typeHint as last inserted in subscriptions with ts $i")
        i
      case Failure(err) ⇒
        log.debug(s"Did not find any $typeHint in subscription collection. Reason $err")
        new ObjectId(new java.util.Date())
    }
    worker ! Subscribe(generateCursor(lastTs))
  }

  val eventSourceHost: String = remoteSourceHost
  val subsClient = MongoClient(eventSourceHost)
  val opLog = subsClient("local")("oplog.rs")
  val localColl = localDb("subscriptions")

  var lastTs: ObjectId = null
  val worker = context.actorOf(
    Props(classOf[SubscriptionWorker[A,B]], translator, serializer, subscriber))

  def generateCursor(l: ObjectId): DBCursor = {
    val query = cursorQuery(l)
    val sort = BasicDBObjectBuilder.start("$natural", 1).get()

    opLog.underlying
      .find(query)
      .sort(sort)
      .addOption(Bytes.QUERYOPTION_TAILABLE)
      .addOption(Bytes.QUERYOPTION_AWAITDATA)
  }

  override def receive: Actor.Receive = {
    case CursorEmpty ⇒
      sender() ! Subscribe(generateCursor(lastTs))
      log.debug(s"Cursor exhausted. Dispatched a new one")
    case DispatchedEvent(ts, event) ⇒
      val receipt:Receipt[Nothing] = try {
        localColl.insert(event)
        lastTs = ts
        log.debug(s"Subscription event was saved to subs collection successfully!")
        Receipt(success=true, updated=None)
      } catch {
        case err: Exception ⇒
          log.error(err, s"Could not insert dispatched subscription event ${event._id} to collection $localColl")
          Receipt.error(err)
      }
      sender() ! receipt
  }
}

class SubscriptionWorker[A <: Event, B]
(translator: A ⇒ B, serializer: Grater[A], subscriber: ActorRef)
  extends Actor with ActorLogging {
  import akka.pattern.ask
  import context.dispatcher

  override def postStop(): Unit = {
    if (currentCursor != null) currentCursor.close()
  }

  implicit val timeout: Timeout = GlobalConfig.ACTOR_TIMEOUT
  var currentCursor: DBCursor = null

  def subscribe(cursor: DBCursor): Unit = {
    currentCursor = cursor
    while (cursor.hasNext) {
      val dbObjectEvent = cursor.next()
      val eventObj = dbObjectEvent.get("o").asInstanceOf[DBObject]
      log.debug(s"Cursor retrieved object $eventObj from ops log")
      val event = serializer.asObject(eventObj)
      val ts = eventObj.as[ObjectId]("_id")
      subscriber.ask(translator(event))
        .mapTo[Receipt[_]]
        .onComplete{
        case Success(receipt) if receipt.success ⇒  context.parent ! DispatchedEvent(ts, eventObj)
        case Success(receiptFalse) ⇒
          throw new FailedToConsumeSubscription(new Exception(receiptFalse.message),s"Could not process dispatched subscription event $event!")
        case Failure(err) ⇒
          throw new FailedToConsumeSubscription(err, s"Could not process dispatched subscription event $event!")
      }
    }
    context.parent ! CursorEmpty
    cursor.close()
  }

  override def preStart(): Unit = {
    log.debug(s"Subscription worker in ${self.path} is ready")
  }

  override def receive: Receive = {
    case Subscribe(cur) ⇒
      log.debug(s"Subscribing using tailable cursor $cur...")
      subscribe(cur)
  }
}
