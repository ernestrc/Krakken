package krakken.system

import akka.actor.{ActorNotFound, AllForOneStrategy, DeathPactException, SupervisorStrategyConfigurator}

class KrakkenGuardianStrategy extends SupervisorStrategyConfigurator {
  def create() = AllForOneStrategy(loggingEnabled = true) {
    case e: DeathPactException ⇒ akka.actor.SupervisorStrategy.Restart
    case e: ActorNotFound ⇒ akka.actor.SupervisorStrategy.Restart
    case e:Exception ⇒ akka.actor.SupervisorStrategy.Restart
  }
}
