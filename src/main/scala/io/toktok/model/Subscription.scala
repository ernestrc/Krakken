package io.toktok.model

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.util.Timeout
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoClient
import com.mongodb.{BasicDBObjectBuilder, Bytes, DBCursor}
import com.novus.salat.Grater
import io.toktok.config.GlobalConfig
import io.toktok.model.SubscriptionMaster.{CursorEmpty, DispatchedEvent, Subscribe, Unsubscribe}
import org.bson.types.BSONTimestamp

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

trait Subscription {
  def unsubscribe(): Unit
}

//TODO throw exception when non-replicaSet?
case class AkkaSubscription[A <: Event : ClassTag, B <: Command : ClassTag]
(serializer: Grater[A], localDb: MongoDB, remoteSourceHost: String)(translator: A ⇒ B)
(implicit context: ActorContext, subscriber: ActorRef, entityId: Option[String]) extends Subscription {

  val subscribedTo = implicitly[ClassTag[A]].runtimeClass
  val subscribedTypeHint = subscribedTo.getCanonicalName

  val master = context.actorOf(Props(classOf[SubscriptionMaster[A,B]],
    translator, subscribedTypeHint, remoteSourceHost, serializer, entityId, localDb, subscriber))

  def unsubscribe(): Unit = master ! Unsubscribe

}

object SubscriptionMaster {

  case object Unsubscribe
  case class Subscribe(cursor: DBCursor)
  case class DispatchedEvent(ts: BSONTimestamp, event: DBObject)
  case object CursorEmpty

}

class SubscriptionMaster[A <: Event, B <: Command](
  translator: A ⇒ B,
  typeHint: String,
  remoteSourceHost: String,
  serializer: Grater[A],
  entityId: Option[String],
  localDb: MongoDB,
  subscriber: ActorRef
) extends Actor with ActorLogging {

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
    case _: Exception => worker ! Subscribe(generateCursor(lastTs)); Restart;
  }

  val initQuery: DBObject = entityId match {
    case Some(id) ⇒
      MongoDBObject(
        "_typeHint" → typeHint,
        "entityId" → entityId
      )
    case _ ⇒ MongoDBObject("_typeHint" → typeHint)
  }

  val cursorQuery = { ts: BSONTimestamp ⇒
    entityId match {
      case Some(id) ⇒ MongoDBObject(
        "ts" → MongoDBObject("$gt" → ts),
        "o._typeHint" → typeHint,
        "o.entityId" → entityId,
        "ns" → MongoDBObject( "$ne" → "toktok.subscriptions")
      )
      case _ ⇒ MongoDBObject(
        "ts" → MongoDBObject("$gt" → ts),
        "o._typeHint" → typeHint,
        "ns" → MongoDBObject( "$ne" → "toktok.subscriptions")
      )
    }
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug(s"Subscribing to events in mongodb://$eventSourceHost of type $typeHint")
    lastTs = Try(localColl.underlying.find(initQuery).sort(BasicDBObjectBuilder.start("_id", -1).get())
      .limit(1).one().as[ObjectId]("_id").getTimestamp) match {
      case Success(i) ⇒
        log.debug(s"Found a $typeHint as last inserted in subscriptions with ts $i")
        new BSONTimestamp(i, 10)
      case Failure(err) ⇒
        log.debug(s"Did not find any $typeHint in subscription collection. Reason $err")
        new BSONTimestamp()
    }
    worker ! Subscribe(generateCursor(lastTs))
  }

  val eventSourceHost: String = remoteSourceHost
  val subsClient = MongoClient(eventSourceHost)
  val opLog = subsClient("local")("oplog.rs")
  val localColl = localDb("subscriptions")

  var lastTs: BSONTimestamp = null
  val worker = context.actorOf(
    Props(classOf[SubscriptionWorker[A,B]], translator, serializer, subscriber))

  def generateCursor(l: BSONTimestamp): DBCursor = {
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
      val receipt:Receipt = try {
        localColl.insert(event)
        lastTs = ts
        log.debug(s"Subscription event was saved to subs collection successfully!")
        Receipt(success=true)
      } catch {
        case err: Exception ⇒
          log.error(err, s"Could not insert dispatched subscription event ${event._id} to collection $localColl")
          Receipt.error(err)
      }
      sender() ! receipt
    case Unsubscribe ⇒ context.stop(self)
  }
}

class SubscriptionWorker[A <: Event, B <: Command]
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
      val event = serializer.asObject(eventObj)
      val ts = dbObjectEvent.as[BSONTimestamp]("ts")
      lazy val logWarn = log.error(s"Could not process dispatched subscription event $event!")
      context.parent.ask(DispatchedEvent(ts, eventObj))
        .mapTo[Receipt]
        .onComplete{
        case Success(receipt) if receipt.success ⇒ subscriber ! translator(event)
        case Success(receiptFalse) ⇒ logWarn
        case Failure(err) ⇒ logWarn
      }
    }
    context.parent ! CursorEmpty
    cursor.close()
  }

  override def preStart(): Unit = {
    log.debug(s"Subscription worker in ${self.path} is ready to rock")
  }

  override def receive: Receive = {
    case Subscribe(cur) ⇒
      log.debug(s"Using tailable cursor $cur...")
      subscribe(cur)
  }
}
