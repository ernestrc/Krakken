package krakken.model

/**
 * Created by ernest on 4/11/15.
 */
object Exceptions {

  abstract class KrakkenException(message: String) extends Exception(message)
  class FailedToConsumeSubscription(err: Throwable, msg: String) extends KrakkenException(msg)

}
