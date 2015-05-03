package krakken.dal

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.util.Timeout
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoClient
import com.mongodb.{BasicDBObjectBuilder, Bytes, DBCursor, DuplicateKeyException}
import com.novus.salat.Grater
import krakken.config.GlobalKrakkenConfig
import krakken.dal.SubscriptionMaster.{CursorEmpty, DispatchedEvent, Subscribe}
import krakken.model.Exceptions.FailedToConsumeSubscription
import krakken.model.{Event, InjectedTypeHint, Receipt, TypeHint}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

trait Subscription {

  val subscribedTo: Class[_]

  def unsubscribe(): Unit
  def subscribe(): Unit
}

//TODO throw exception when non-replicaSet?
case class AkkaSubscription[A <: Event : ClassTag, B : ClassTag]
(serializer: Grater[A], localDb: MongoDB, remoteSourceHost: String, remoteDbName: String)(translator: A ⇒ B)
(implicit context: ActorContext, subscriber: ActorRef, entityId: Option[String]) extends Subscription {

  private var isAlive = false

  val subscribedTo = implicitly[ClassTag[A]].runtimeClass
  val subscribedTypeHint = InjectedTypeHint(subscribedTo.getCanonicalName)

  private var master = context.system.deadLetters

  def unsubscribe(): Unit = {
    if(isAlive) {
      context.stop(master)
      isAlive = false
    }
  }
  def subscribe(): Unit = {
    if(isAlive) {
      context.stop(master)
    }
    context.system.log.debug("AkkaSubscription starting subscription process now...")
    master = context.actorOf(Props(classOf[SubscriptionMaster[A,B]],
      translator, subscribedTypeHint, remoteSourceHost, remoteDbName, serializer, entityId, localDb, subscriber))
    isAlive = true
  }

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
  typeHint: TypeHint,
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
        "_typeHint" → typeHint.hint,
        "entityId" → entityId
      )
    case _ ⇒ MongoDBObject("_typeHint" → typeHint.hint)
  }

  val cursorQuery = { ts: ObjectId ⇒
    entityId match {
      case Some(id) ⇒ MongoDBObject(
        "o._id" → MongoDBObject("$gt" → ts),
        "o._typeHint" → typeHint.hint,
        "o.entityId" → entityId,
        "ns" → MongoDBObject( "$ne" →
          s"${localDb.getName}.${classOf[Subscription].getSimpleName}"
        )
      )
      case _ ⇒ MongoDBObject(
        "o._id" → MongoDBObject("$gt" → ts),
        "o._typeHint" → typeHint.hint,
        "ns" → MongoDBObject("$ne" →
          s"${localDb.getName}.${classOf[Subscription].getSimpleName}")
      )
    }
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug(s"Subscribing to events in mongodb://$eventSourceHost of type $typeHint")
    lastTs = Try(localColl.underlying.find(initQuery).sort(BasicDBObjectBuilder.start("_id", -1).get())
      .limit(1).one().as[ObjectId]("_id")) match {
      case Success(i) ⇒
        log.debug("Found a {} as last inserted in subscriptions with ts {}", typeHint, i)
        i
      case Failure(err) ⇒
        log.debug("Did not find any {} in subscription collection. Reason {}", typeHint, err)
        new ObjectId(new java.util.Date(1))
    }
    worker ! Subscribe(generateCursor(lastTs))
  }

  val eventSourceHost: String = remoteSourceHost
  val subsClient = MongoClient(eventSourceHost)
  val opLog = subsClient("local")("oplog.rs")
  val localColl = localDb(classOf[Subscription].getSimpleName)

  var lastTs: ObjectId = null
  val worker = context.actorOf(
    Props(classOf[SubscriptionWorker[A,B]], translator, serializer, subscriber))

  def generateCursor(l: ObjectId): DBCursor = {
    val query = cursorQuery(l)
    val sort = BasicDBObjectBuilder.start("$natural", 1).get()

    log.debug("SubscriptionMaster of {} generated query cursor {}", typeHint.hint, query)

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
      val receipt:Receipt[_] = try {
        localColl.insert(event)
        lastTs = ts
        log.debug(s"Subscription event was saved to subs collection successfully!")
        Receipt(success=true, entity=event)
      } catch {
        case err: DuplicateKeyException ⇒
          log.debug("Could not insert subscription because it was already inserted")
          Receipt(success=true, entity=event)
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

  implicit val timeout: Timeout = GlobalKrakkenConfig.ACTOR_TIMEOUT
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
