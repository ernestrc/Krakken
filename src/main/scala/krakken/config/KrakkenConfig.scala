package krakken.config

import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}
import krakken.io
import io._
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try

class KrakkenConfig {

  val log = LoggerFactory.getLogger(this.getClass)
  
  protected val config: Config = ConfigFactory.load()

  /* lazy not ideal, but necessary to give a bit more flexibility
  * in case a source is not needed */
  lazy val dataContainer = config.getString("krakken.source.container")
  lazy val mongoHost: String = config.getString("krakken.source.host")
  lazy val mongoPort: Int = config.getInt("krakken.source.port")
  lazy val dbName = config.getString("krakken.source.db")

  def collectionsDB(collection: String) = config.getString(s"krakken.source.collections.$collection.db")

  def collectionsPort(collection: String) = config.getString(s"krakken.source.collections.$collection.port")

  def collectionsHost(collection: String): String =
    getContainerLink(config.getString(s"krakken.source.collections.$collection.container"))
      .map(_.host.ip)
      .getOrElse(config.getString(s"krakken.source.collections.$collection.host"))

  val ACTOR_TIMEOUT: FiniteDuration =
    FiniteDuration(config.getDuration("krakken.actors.timeout",
      TimeUnit.SECONDS), TimeUnit.SECONDS)

  val REGISTRATION_TTL: Int = config.getInt("krakken.etcd.registration-ttl")

  val ETCD_POLLING: FiniteDuration = {
    val conf = config.getString("spray.can.client.request-timeout")
    val duration = if (conf == "infinite") -1 else config.getDuration("spray.can.client.request-timeout", TimeUnit.SECONDS)
    Duration(duration, TimeUnit.SECONDS)
  }

  val ETCD: String = Try(config.getString("krakken.etcd-env")).toOption.getOrElse("ETCD_ENDPOINT")

}

private[krakken] object GlobalKrakkenConfig extends KrakkenConfig