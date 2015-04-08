package io.toktok.service

import akka.actor.{Actor, ActorLogging}
import io.toktok.dal.MongoSource
import io.toktok.model.Exceptions.TokTokException
import io.toktok.model.{Command, Event, Receipt}

/**
 * Created by ernest on 4/2/15.
 */
abstract class EventSourcedActor[T <: Event] extends Actor with ActorLogging {

  val name: String = self.path.name

  val entityId: String

  val source: MongoSource[T]

  def eventProcessor: PartialFunction[Event, Unit]

  def commandProcessor: PartialFunction[Command, List[T]]

  def receive: Receive = {
    case cmd: Command ⇒
      val receipt: Receipt = try {
        val events = commandProcessor(cmd)
        events.foreach(source.save)
        events.foreach(eventProcessor)
        Receipt(success = true, updated = entityId, message = "OK")
      } catch {
        case ex: TokTokException ⇒
          Receipt.error(ex)
        case err: Exception ⇒
          log.error(err, s"There was an error when processing command $cmd!")
          Receipt.error(err)
      }
      sender() ! receipt
      log.debug(s"Actor $name processed $cmd")
    case anyElse ⇒ log.error(s"Oops, it looks like I shouldn't have received $anyElse")
  }
}
