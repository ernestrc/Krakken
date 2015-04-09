//package io.toktok.auth
//
//import org.slf4j.LoggerFactory
//import shapeless.HNil
//import spray.http.{HttpHeader, HttpHeaders}
//import spray.routing.Directives._
//import spray.routing.{Directive0, Directive1, RequestContext}
//import spray.util._
//
///**
// * Created by ernest on 4/9/15.
// */
//trait AuthenticationProvider {
//  val actionContext: Directive0
//}
//
//class TokenAuthentication extends AuthenticationProvider {
//
//  val log = LoggerFactory.getLogger(classOf[TokenAuthentication])
//
//  val PREFIX = "ActionContext "
//
//  def acHeaderPreCheck(value: String) = value.length < 255 && value.startsWith(PREFIX)
//
//  def acHeaderSanitise(value: String) = value.trim.substring(PREFIX.length)
//
//  def rcToAuthHeader(rc: RequestContext) = rc.request.headers.mapFind {
//    case HttpHeader(HttpHeaders.Authorization.lowercaseName, value) if acHeaderPreCheck(value) =>
//      Some(acHeaderSanitise(value))
//    case _ => None
//  }
//
//  def rcToMeta(rc:RequestContext) = Some(new LazyConnectionMeta(rc.request))
//
//  val uriAndAuthHeader: Directive1[(Option[String], Option[ConnectionMeta])] = extract(
//    rc => (rcToAuthHeader(rc), rcToMeta(rc))
//  )
//
//  val UserACPattern = "user\\(([0-9]+)\\)".r
//  val SuperACPattern = "super\\(([0-9]+)\\)".r
//
//  def illegalContext() = {
//    log.warn("FAILED AUTHENTICATION! Illegal ActionContext!")
//    throw new IllegalArgumentException("Illegal ActionContext!")
//  }
//
//  val actionContext: Directive0 = uriAndAuthHeader.map{
//    case (Some(UserACPattern(userId)), meta) =>
//    case (_, meta) => HNil
//    case _ ⇒ HNil
//
//  }
//}