package krakken.http

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import krakken.config.GlobalKrakkenConfig
import krakken.http.SystemOverseer.{CheckStatus, Restart}

class SystemOverseer(handlerProps: Props) extends Actor with ActorLogging {

  import context.dispatcher

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(){
    case e: Exception ⇒
      log.warning(s"Stopping http handler! Http interface is down but will be up again in ${GlobalKrakkenConfig.OVERSEER_RESTART}..")
      context.become(error(e))
      //schedule restart
      context.system.scheduler.scheduleOnce(GlobalKrakkenConfig.OVERSEER_RESTART, self, SystemOverseer.Restart)
      Stop
  }

  var pending: List[Any] = List.empty[Any]

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.info(s"HttpOverseer is up and running in path ${self.path}")
  }


  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    log.warning(s"Restarting ${self.path}. Reason $reason")
    preStart()
  }

  def newHandler: ActorRef = context.actorOf(handlerProps, "handler")

  def error(ex: Exception): Receive = {
    case CheckStatus ⇒ sender ! ex.toString
    case Restart ⇒
      val handler = newHandler
      pending.foreach(handler ! _)
      pending = List.empty[Any]
      context.become(ready(handler))
    case e ⇒ pending ::= e
  }

  def ready(handler: ActorRef): Receive = {
    case CheckStatus ⇒
      log.debug("Checking status of handler")
      sender ! "OK"
    case e ⇒ handler forward e
  }

  override def receive: Receive = ready(newHandler)
}

object SystemOverseer {
  
  case object CheckStatus
  case object Restart
  case class Status(ok: Boolean)

  def props(httpHandler: Props): Props =
    Props(classOf[SystemOverseer], httpHandler)
}
