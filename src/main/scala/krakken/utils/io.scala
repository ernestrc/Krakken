package krakken.utils

import scala.io.Source
import scala.util.Try

object io {

  case class Host(alias: String, ip: String){
    def toContainer:Option[Container]= {
      getContainerLink(alias)
    }
  }

  def loadHosts(hosts: String = "/etc/hosts"): List[Host] = {
    val h = Source.fromFile(hosts)
    val raw = h.mkString.split('\n').filterNot(_.startsWith("#"))
    raw.map { entry ⇒
      val e = entry.split("\t")
      Try(Host(e(1), e(0))).getOrElse(Host(e(0), e(0)))
    }.toList
  }

  case class Container(host: Host, port: Int)

  def getContainerLink(name: String): Option[Container] = Try {
    lazy val connection = System.getenv(s"${name}_PORT").split(':')
    lazy val port:Int = connection.last.toInt
    loadHosts().find(_.alias == name).map{ host ⇒
      Container(host, port)
    }.getOrElse {
      val port = connection.last.toInt
      val host = connection.dropRight(1).last
      Container(Host(name, host), port)
    }
  }.toOption
}
