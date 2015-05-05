package krakken.http

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import krakken.config.GlobalKrakkenConfig
import krakken.http.SystemOverseer.{CheckStatus, Restart}

class SystemOverseer(handlerProps: Props) extends Actor with ActorLogging {

  import context.dispatcher

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(){
    case e:Exception ⇒
      context.become(error)
      //schedule restart
      context.system.scheduler.scheduleOnce(GlobalKrakkenConfig.OVERSEER_RESTART, self, SystemOverseer.Restart)
      Stop
  }

  var handler: ActorRef = newHandler
  var pending: List[Any] = List.empty[Any]

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.info("HttpOverseer is up and running")
  }

  def newHandler: ActorRef = context.actorOf(handlerProps, "handler")

  def error: Receive = {
    case CheckStatus ⇒ sender ! "ERROR"
    case Restart ⇒
      handler = newHandler
      pending.foreach(handler ! _)
      pending = List.empty[Any]
      context.become(ready)
    case e ⇒ pending ::= e
  }

  def ready: Receive = {
    case CheckStatus ⇒ sender ! "OK"
    case e ⇒ handler forward e
  }

  override def receive: Receive = ready
}

object SystemOverseer {
  
  case object CheckStatus
  case object Restart
  case class Status(ok: Boolean)

  def props(httpHandler: Props): Props =
    Props(classOf[SystemOverseer], httpHandler)
}
