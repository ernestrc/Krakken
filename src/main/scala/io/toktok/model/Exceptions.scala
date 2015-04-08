package io.toktok.model

/**
 * Created by ernest on 4/2/15.
 */
object Exceptions {

  sealed class TokTokException(message: String) extends Exception(message)
  case class UserExistsException(username: String) extends TokTokException(s"User $username already exists")
  class WrongPasswordException extends TokTokException("Wrong password!")

}
