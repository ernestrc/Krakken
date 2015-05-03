package krakken.io

import spray.http.Uri

case class Service(host: Host, port: Int){

  lazy val url:String = host.ip + ":" + port

  def getPathUri(path: String, securedConnection: Boolean = false): Uri =
  Uri.effectiveHttpRequestUri("http", Uri.Host(host.ip), port, Uri.Path(path),
    Uri.Query.apply(None), None, securedConnection, Uri.Host(host.ip), port)

  override def toString: String = s"${host.alias}[${host.ip}:$port]"
}
