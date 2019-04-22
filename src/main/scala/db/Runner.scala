package db

import akka.actor.typed.{ DispatcherSelector, Terminated }
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import db.core.{ DbReplica, KeyValueStorageBackend2 }

import scala.concurrent.duration._

//runMain db.Runner
object Runner extends App {

  val systemName = "db"

  val config = ConfigFactory.parseString(
    s"""
       akka {
          cluster {
            roles = [ db-replica ]
            jmx.multi-mbeans-in-same-jvm = on
            shutdown-after-unsuccessful-join-seed-nodes = 30s
          }

          actor.provider = cluster

          remote.artery.enabled = true
          remote.artery.transport = tcp
          remote.artery.canonical.hostname = 127.0.0.1
       }
      """)

  def portConfig(port: Int) =
    ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port")

  /*
    The number of failures that can be tolerated is equal to (Replication factor - 1) /2.
    For example, with 3x replication, one failure can be tolerated; with 5x replication, two failures, and so on.
  */

  val RF = 3
  val CL = 3
  val ticketNmr = 500000

  import akka.actor.typed.scaladsl.adapter._

  def alphaSys = akka.actor.typed.ActorSystem(
    //guardian
    Behaviors.setup[Unit] { ctx ⇒
      val replica = ctx.spawn(DbReplica(RF, CL, 0l), "alpha-replica", DispatcherSelector.fromConfig("akka.db-io"))
      ctx.watch(replica)

      Behaviors.receiveSignal {
        case (_, Terminated(`replica`)) ⇒
          ctx.log.error("★ ★ ★ ★ ★ ★  Replica 0: Failure detected")
          Behaviors.stopped
      }
    },
    systemName, portConfig(2550).withFallback(config).withFallback(ConfigFactory.load()))

  def bettaSys = akka.actor.typed.ActorSystem(
    //guardian
    Behaviors.setup[Unit] { ctx ⇒
      val replica = ctx.spawn(DbReplica(RF, CL, 1l), "betta-replica", DispatcherSelector.fromConfig("akka.db-io"))
      ctx.watch(replica)

      Behaviors.receiveSignal {
        case (_, Terminated(`replica`)) ⇒
          ctx.log.error("★ ★ ★ ★ ★ ★  Replica 1: Failure detected")
          Behaviors.stopped
      }
    },
    systemName, portConfig(2551).withFallback(config).withFallback(ConfigFactory.load()))

  def gammaSys = akka.actor.typed.ActorSystem(
    //guardian
    Behaviors.setup[Unit] { ctx ⇒
      val replica = ctx.spawn(DbReplica(RF, CL, 2l), "gamma-replica", DispatcherSelector.fromConfig("akka.db-io"))
      ctx.watch(replica)

      Behaviors.receiveSignal {
        case (_, Terminated(`replica`)) ⇒
          ctx.log.error("★ ★ ★ ★ ★ ★  Replica 2: Failure detected")
          Behaviors.stopped
      }
    },
    systemName, portConfig(2552).withFallback(config).withFallback(ConfigFactory.load()))

  val as = alphaSys
  val alpha = Cluster(as.toUntyped)

  val bs = bettaSys
  val betta = Cluster(bs.toUntyped)

  val gs = gammaSys
  val gamma = Cluster(gs.toUntyped)

  alpha.join(alpha.selfAddress)
  betta.join(alpha.selfAddress)
  gamma.join(alpha.selfAddress)

  Helpers.waitForAllNodesUp(as.toUntyped, bs.toUntyped, gs.toUntyped)

  /*
  alphaSys.actorOf(KeyValueStorageBackend2.props, DB.PathSegment)
  bettaSys.actorOf(KeyValueStorageBackend2.props, DB.PathSegment)
  gammaSys.actorOf(KeyValueStorageBackend2.props, DB.PathSegment)
  */

  /*Helpers.wait(20.second)

  println("****************** Kill gamma *********************")
  gamma.leave(gamma.selfAddress)
  gammaSys.terminate

  Helpers.wait(20.second)

  println("****************** new incarnation of gamma joins the cluster *********************")
  val gammaSys2 = ActorSystem(systemName, portConfig(2552).withFallback(config).withFallback(ConfigFactory.load()))
  val gamma2 = Cluster(gammaSys2)
  gamma2.join(alpha.selfAddress)
  gammaSys2.actorOf(DB.props(gamma2, 2l, RF, CL), "gamma")
  gammaSys2.actorOf(KeyValueStorageBackend2.props, DB.PathSegment)

  Helpers.waitForAllNodesUp(alphaSys, bettaSys, gammaSys2)*/

  Helpers.wait(10.second)
  println("★ ★ ★  gamma partitioned ★ ★ ★")
  //gamma.leave(gamma.selfAddress)
  gs.terminate

  /*Helpers.wait(20.second)
  println("★ ★ ★  betta killed  ★ ★ ★")
  betta.leave(betta.selfAddress)
  bettaSys.terminate*/

  Helpers.wait(20.second)
  println("★ ★ ★  betta partitioned  ★ ★ ★")
  //betta.leave(betta.selfAddress)
  bs.terminate

  Helpers.wait(30.second)

  alpha.leave(alpha.selfAddress)
  as.terminate

  /*Helpers.wait(20.second)
  println("★ ★ ★  alpha patritioned  ★ ★ ★")
  as.terminate

  Helpers.wait(20.second)

  betta.leave(alpha.selfAddress)
  bs.terminate*/

}