package io.toktok.http

import akka.actor.ActorRefFactory
import akka.event.Logging
import io.toktok.model.Receipt
import io.toktok.utils.Implicits
import io.toktok.utils.Implicits._
import spray.http.StatusCode
import spray.http.StatusCodes._
import spray.routing._
import spray.util.LoggingContext

trait HttpConfig {

  val exceptionHandler: ExceptionHandler

  val rejectionHandler: RejectionHandler

  val routingSettings: RoutingSettings

  val loggingContext: LoggingContext

}

trait DefaultHttpConfig extends HttpConfig {

  implicit val receiptGrater = graterMarshallerConverter(Receipt.receiptGrater)

  implicit def actorRefFactory: ActorRefFactory

  private def loggedFailureResponse(ctx: RequestContext, thrown: Throwable,
                                    message: String = "Something Exploded!",
                                    error: StatusCode = InternalServerError,
                                    logLevel: Logging.LogLevel = Logging.ErrorLevel): Unit = {
    val msg = s"$message CAUSE $thrown"
    loggingContext.log(logLevel, msg)
    ctx.complete((error, Receipt.error(thrown, msg)))
  }

  val exceptionHandler: ExceptionHandler = ExceptionHandler {

    case e: IllegalArgumentException => ctx =>
      loggedFailureResponse(ctx, e,
        message = "Illegan argument Exception thrown: " + e.getMessage,
        error = NotAcceptable,
        Logging.WarningLevel)

    case e: NoSuchElementException => ctx =>
      loggedFailureResponse(ctx, e,
        message = "Not Found",
        error = NotFound,
        Logging.InfoLevel
      )

    case t: Throwable => ctx =>
      loggedFailureResponse(ctx, t)
  }

  val routingSettings: RoutingSettings = RoutingSettings.default
  val loggingContext: LoggingContext = LoggingContext.fromActorRefFactory
  val rejectionHandler: RejectionHandler = RejectionHandler.Default

}
