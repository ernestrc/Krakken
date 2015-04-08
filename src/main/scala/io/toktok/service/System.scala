package io.toktok.service

import akka.actor.ActorSystem


/**
 * System is type containing the ``system: ActorSystem`` member. This enables us to use it in our
 * apps as well as in our tests.
 */
trait System {

  implicit def system: ActorSystem

}

/**
 * This trait implements ``Core`` by starting the required ``ActorSystem`` and registering the
 * termination handler to stop the system when the JVM exits.
 */
trait BootedSystem extends System {

  implicit val system: ActorSystem = ActorSystem(name = "toktok")

  sys.addShutdownHook(system.shutdown())

}

