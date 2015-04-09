//package io.toktok.auth
//
//import spray.http.{HttpHeader, HttpRequest}
//import spray.json.{JsObject, JsString}
//import spray.util._
//
//
//trait ConnectionMeta {
//  def ip: Option[String]
//
//  def userAgent: Option[String]
//
//  def session: Option[String]
//
//  lazy val json = JsObject((Nil ++
//    ip.map("clientIp" -> JsString(_)) ++
//    userAgent.map("userAgent" -> JsString(_)) ++
//    session.map("session" -> JsString(_))
//    ).toMap)
//}
//
//class LazyConnectionMeta(request: HttpRequest) extends ConnectionMeta {
//  def isRemoteAddressHeader(h: HttpHeader) = h.is("x-real-ip") || h.is("x-forwarded-for") || h.is("remote-address")
//
//  def isRealAgentHeader(h: HttpHeader) = h.is("x-real-user-agent")
//
//  def isBrowserSessionHeader(h: HttpHeader) = h.is("x-session")
//
//  override lazy val ip: Option[String] = request.headers.mapFind {
//    case h: HttpHeader if isRemoteAddressHeader(h) => Some(h.value)
//    case _ => None
//  }
//
//  override def userAgent: Option[String] = request.headers.mapFind {
//    case h: HttpHeader if isRealAgentHeader(h) => Some(h.value)
//    case _ => None
//  }
//
//  override def session: Option[String] = request.headers.mapFind {
//    case h: HttpHeader if isBrowserSessionHeader(h) => Some(h.value)
//    case _ => None
//  }
//}
//
//trait ActionContext {
//
//}