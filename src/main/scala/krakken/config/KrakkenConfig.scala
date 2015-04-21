package krakken.config

import java.util.concurrent.TimeUnit

import akka.event.{Logging, LoggingAdapter}
import com.typesafe.config.{Config, ConfigFactory}
import krakken.utils.io._
import org.jboss.netty.logging.Slf4JLoggerFactory
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration

class KrakkenConfig {

  val log = LoggerFactory.getLogger(this.getClass)

  implicit class containerToAkkaUrl(s: Container) {
    def toAkkaUrl: String =
      s"akka.tcp://${s.host.alias}@${s.host.ip}:${s.port}/user"
  }
  
  protected val config: Config = ConfigFactory.load()

  val links: List[Container] = loadHosts().map(_.toContainer).flatten

  def collectionsDB(collection: String) = config.getString(s"krakken.source.collections.$collection.db")

  def collectionsPort(collection: String) = config.getString(s"krakken.source.collections.$collection.port")

  def collectionsHost(collection: String): String =
    links.find(_.host.alias == config.getString(s"krakken.source.collections.$collection.container"))
      .map(_.host.ip)
      .getOrElse(config.getString(s"krakken.source.collections.$collection.host"))

  val ACTOR_TIMEOUT: FiniteDuration =
    FiniteDuration(config.getDuration("krakken.actors.timeout",
      TimeUnit.SECONDS), TimeUnit.SECONDS)

  log.debug("Loaded KrakkenConfig with links {}", links)

}

private[krakken] object GlobalKrakkenConfig extends KrakkenConfig