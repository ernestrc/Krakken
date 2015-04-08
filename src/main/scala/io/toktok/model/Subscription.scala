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
import model.SID
import org.bson.types.BSONTimestamp

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

trait Subscription {
  def unsubscribe(): Unit
}

//TODO throw exception when non-replicaSet?
case class AkkaSubscription[A <: B : ClassTag, B <: Event : ClassTag]
(serializer: Grater[A], entityId: SID)(callback: A ⇒ Any)(implicit context: ActorContext) extends Subscription {

  val subscribedTo = implicitly[ClassTag[A]].runtimeClass
  val typeHint = subscribedTo.getCanonicalName
  val parentType = implicitly[ClassTag[B]].runtimeClass.getSimpleName

  val master = context.actorOf(Props(classOf[SubscriptionMaster[A]],
    callback, typeHint, parentType, serializer, entityId))

  def unsubscribe(): Unit = master ! Unsubscribe

}

object SubscriptionMaster {

  case object Unsubscribe
  case class Subscribe(cursor: DBCursor)
  case class DispatchedEvent(ts: BSONTimestamp, event: DBObject)
  case object CursorEmpty

}

class SubscriptionMaster[A <: Event](
  callback: A ⇒ Any,
  typeHint: String,
  parentType: String,
  serializer: Grater[A],
  entityId: SID
) extends Actor with ActorLogging {

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
    case _: Exception => worker ! Subscribe(generateCursor(lastTs)); Restart;
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug(s"Subscribing to events in mongodb://$eventSourceHost in col $parentType of type $typeHint")
    val ts = Try(localColl.underlying.find(
        MongoDBObject(
            "o._typeHint" → typeHint,
            "o.entityId" → entityId)
    ).sort(BasicDBObjectBuilder.start("_id", -1).get())
    .limit(1).one().as[ObjectId]("_id").getTimestamp)
    lastTs = ts.map(t ⇒ new BSONTimestamp(t, 0)).getOrElse(new BSONTimestamp())
    log.debug(s"Last object of $typeHint was inserted was on $lastTs")
    worker ! Subscribe(generateCursor(lastTs))
  }

  val eventSourceHost: String = GlobalConfig.collectionsHost(parentType)
  val subsClient = MongoClient(eventSourceHost)
  val opLog = subsClient("local")("oplog.rs")
  val localClient = MongoClient(GlobalConfig.mongoHost)
  val localColl = localClient(GlobalConfig.mongoDb)(parentType)

  var lastTs: BSONTimestamp = null
  val worker = context.actorOf(
    Props(classOf[SubscriptionWorker[A]], callback, serializer))

  def generateCursor(l: BSONTimestamp): DBCursor = {
    val query = MongoDBObject(
      "ts" → MongoDBObject("$gt" → l),
      "o._typeHint" → typeHint,
      "o.entityId" → entityId
    )
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

class SubscriptionWorker[A <: Event](callback: A ⇒ Any, serializer: Grater[A]) 
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
      lazy val logWarn = log.warning(s"Could not process dispatched subscription event $event!")
      context.parent.ask(DispatchedEvent(ts, eventObj))
        .mapTo[Receipt]
        .onComplete{
        case Success(receipt) if receipt.success ⇒ callback(event)
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
