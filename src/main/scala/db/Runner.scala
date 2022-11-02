package db

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed._
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import db.core.{HashRing, MVCCStorageBackend, Replica}
import db.hashing.Rendezvous

import scala.concurrent.duration._

//runMain db.Runner
object Runner extends App {

  val systemName   = "db"
  val DBDispatcher = "akka.db-io"

  val config = ConfigFactory.parseString(
    s"""
       akka {
          cluster {
            roles = [ db-replica ]
            jmx.multi-mbeans-in-same-jvm = on
            shutdown-after-unsuccessful-join-seed-nodes = 30s
          }

          actor.provider = cluster
          remote.artery.canonical.hostname = 127.0.0.1
       }
      """
  )

  def portConfig(port: Int) =
    ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port")

  val RF        = 3
  val ticketNmr = 500000

  def alphaSys: ActorSystem[Nothing] =
    ActorSystem[Nothing](
      // guardian
      Behaviors
        .setup[Unit] { ctx =>
          val replica = ctx.spawn(HashRing(RF, 0L), HashRing.Name, DispatcherSelector.fromConfig(DBDispatcher))

          ctx.actorOf(MVCCStorageBackend.props(ctx.system.receptionist), "sb")
          // .toTyped[MVCCStorageBackend.ReservationReply]

          // Behaviors.supervise(storage).onFailure[Exception](SupervisorStrategy.resume.withLoggingEnabled(true))

          ctx.watch(replica)

          Behaviors.receiveSignal {
            case (_, PostStop) =>
              Behaviors.same
            case (_, ChildFailed((replica, cause))) =>
              ctx.log.error(s"★ ★ ★ ★ ★ ★  Replica 0: ChildFailed $replica", cause)
              Behaviors.same
            case (_, Terminated(`replica`)) =>
              ctx.log.error("★ ★ ★ ★ ★ ★  Replica 0: Failure detected")
              Behaviors.stopped
          }
        }
        .narrow,
      systemName,
      portConfig(2550).withFallback(config).withFallback(ConfigFactory.load())
    )

  def bettaSys: ActorSystem[Nothing] =
    ActorSystem[Nothing](
      // guardian
      Behaviors
        .setup[Unit] { ctx =>
          val replica = ctx.spawn(HashRing(RF, 1L), HashRing.Name, DispatcherSelector.fromConfig(DBDispatcher))
          ctx.watch(replica)

          ctx.actorOf(MVCCStorageBackend.props(ctx.system.receptionist), "sb")

          Behaviors.receiveSignal { case (_, Terminated(`replica`)) =>
            ctx.log.error("★ ★ ★ ★ ★ ★  Replica 1: Failure detected")
            Behaviors.stopped
          }
        }
        .narrow,
      systemName,
      portConfig(2551).withFallback(config).withFallback(ConfigFactory.load())
    )

  def gammaSys: ActorSystem[Nothing] =
    ActorSystem[Nothing](
      // guardian
      Behaviors
        .setup[Unit] { ctx =>
          val replica = ctx.spawn(HashRing(RF, 2L), HashRing.Name, DispatcherSelector.fromConfig(DBDispatcher))
          ctx.watch(replica)

          ctx.actorOf(MVCCStorageBackend.props(ctx.system.receptionist), "sb")

          Behaviors.receiveSignal { case (_, Terminated(`replica`)) =>
            ctx.log.error("★ ★ ★ ★ ★ ★  Replica 2: Failure detected")
            Behaviors.stopped
          }
        }
        .narrow,
      systemName,
      portConfig(2552).withFallback(config).withFallback(ConfigFactory.load())
    )

  val as    = alphaSys
  val alpha = Cluster(as.toClassic)

  val bs    = bettaSys
  val betta = Cluster(bs.toClassic)

  val gs    = gammaSys
  val gamma = Cluster(gs.toClassic)

  alpha.join(alpha.selfAddress)
  betta.join(alpha.selfAddress)
  gamma.join(alpha.selfAddress)

  Helpers.waitForAllNodesUp(as.toClassic, bs.toClassic, gs.toClassic)

  // Helpers.wait(60.second)

  Thread.sleep(10000)
  gamma.leave(gamma.selfAddress)
  gs.terminate

  Thread.sleep(10000)
  betta.leave(betta.selfAddress)
  bs.terminate

  Thread.sleep(10000)
  alpha.leave(alpha.selfAddress)
  as.terminate

  /*
  Helpers.wait(60.second)
  println("★ ★ ★  gamma partitioned ★ ★ ★")
  //gamma.leave(gamma.selfAddress)
  gs.terminate

  Helpers.wait(10.second)
  println("★ ★ ★  betta partitioned  ★ ★ ★")
  //betta.leave(betta.selfAddress)
  bs.terminate

  Helpers.wait(10.second)
  alpha.leave(alpha.selfAddress)
  as.terminate
   */
}
