package io.toktok.config

import java.util.concurrent.TimeUnit

import com.typesafe.config.{ConfigFactory, Config}

import scala.concurrent.duration.FiniteDuration

object GlobalConfig {

  private val config: Config = ConfigFactory.load()

  val mongoHost = config.getString("toktok.source.host")

  val mongoDb = config.getString("toktok.source.db")

  def collectionsHost(collection: String) = config.getString(s"toktok.source.collections.$collection.host")

  val ACTOR_TIMEOUT: FiniteDuration =
    FiniteDuration(config.getDuration("toktok.actors.timeout",
      TimeUnit.SECONDS), TimeUnit.SECONDS)

  val akkaRemoteHost: String = config.getString("akka.remote.netty.tcp.hostname")

  val akkaRemotePort: Int = config.getInt("akka.remote.netty.tcp.port")

}
