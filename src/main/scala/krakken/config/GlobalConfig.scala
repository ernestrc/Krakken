package krakken.config

import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.FiniteDuration

object GlobalConfig {

  private val config: Config = ConfigFactory.load()

  lazy val mongoHost = config.getString("toktok.source.host")

  lazy val mongoDb = config.getString("toktok.source.db")

  def collectionsHost(collection: String) = config.getString(s"toktok.source.collections.$collection.host")

  def collectionsDB(collection: String) = config.getString(s"toktok.source.collections.$collection.db")

  def collectionsPort(collection: String) = config.getString(s"toktok.source.collections.$collection.port")

  val ACTOR_TIMEOUT: FiniteDuration =
    FiniteDuration(config.getDuration("toktok.actors.timeout",
      TimeUnit.SECONDS), TimeUnit.SECONDS)

  val akkaRemoteHost: String = config.getString("akka.remote.netty.tcp.hostname")

  val akkaRemotePort: Int = config.getInt("akka.remote.netty.tcp.port")

}