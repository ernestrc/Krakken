package krakken.utils

import scala.io.Source
import scala.util.Try

object io {

  implicit class containerToAkkaUrl(s: Container) {
    def toAkkaUrl: String =
      s"akka.tcp://${s.host.alias}@${s.host.ip}:${s.port}/user"
  }

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

  /**
   * It will try to find the container from etcd. If it fails, it will try to find
   * the container as a docker linked container. If it fails, it will try to locate
   * the container via standard config.
   * @param name container name
   * @return option of container
   */
  def getContainerLink(name: String): Option[Container] = Try {
    lazy val connection = System.getenv(s"${name.toUpperCase}_PORT").split(':')
    lazy val port:Int = connection.last.toInt
    loadHosts().find(_.alias == name).map{ host ⇒
      Container(host, port)
    }.getOrElse {
      val port = connection.last.toInt
      val host = connection.dropRight(1).last.stripPrefix("//")
      Container(Host(name, host), port)
    }
  }.toOption
}
