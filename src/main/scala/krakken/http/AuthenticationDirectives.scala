package krakken.http

import spray.routing.AuthenticationFailedRejection
import spray.routing.AuthenticationFailedRejection.CredentialsRejected
import spray.routing.authentication.{Authentication, ContextAuthenticator}

import scala.concurrent.{ExecutionContext, Future}

trait AuthenticationDirectives {

  def authenticateUserWithApiKey(keys: List[String])(implicit exc: ExecutionContext): ContextAuthenticator[Service] = {
    def doAuth(key: String)(implicit exc: ExecutionContext): Future[Authentication[Service]] = {
      Future {
        Either.cond(keys.contains(key),
          Service(key = key),
          AuthenticationFailedRejection(CredentialsRejected, List.empty))
      }
    }
    ctx => {
      val key = ctx.request.uri.query.get("key").getOrElse("")
      doAuth(key)
    }
  }

  case class Service(key: String)

}
