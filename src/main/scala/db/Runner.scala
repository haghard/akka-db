package db

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import db.core.{ DB, KeyValueStorageBackend2 }
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

            //This means that the cluster leader member will change the unreachable node status to down automatically after the configured time of unreachability.
            //auto-down-unreachable-after = 2 s

            shutdown-after-unsuccessful-join-seed-nodes = 30s
          }

          actor.provider = cluster

          remote.artery.enabled = true
          #remote.artery.transport = tcp
          remote.artery.canonical.hostname = 127.0.0.1

          db-io {
           type = "Dispatcher"
           executor = "fork-join-executor"
           fork-join-executor {
             parallelism-min = 2
             parallelism-max = 4
           }
         }
       }
      """)

  def portConfig(port: Int) =
    ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port")

  //ConfigFactory.parseString(s"akka.remote.netty.tcp.port = $port")

  /*
    The number of failures that can be tolerated is equal to (Replication factor - 1) /2.
    For example, with 3x replication, one failure can be tolerated; with 5x replication, two failures, and so on.
  */

  val RF = 3
  val CL = 3
  val ticketNmr = 500000

  val alphaSys = ActorSystem(systemName, portConfig(2550).withFallback(config).withFallback(ConfigFactory.load()))
  val bettaSys = ActorSystem(systemName, portConfig(2551).withFallback(config).withFallback(ConfigFactory.load()))
  val gammaSys = ActorSystem(systemName, portConfig(2552).withFallback(config).withFallback(ConfigFactory.load()))

  val alpha = Cluster(alphaSys)
  val betta = Cluster(bettaSys)
  val gamma = Cluster(gammaSys)

  alpha.join(alpha.selfAddress)
  betta.join(alpha.selfAddress)
  gamma.join(alpha.selfAddress)

  Helpers.waitForAllNodesUp(alphaSys, bettaSys, gammaSys)

  alphaSys.actorOf(DB.props(alpha, 0l, RF, CL), "alpha")
  bettaSys.actorOf(DB.props(betta, 1l, RF, CL), "betta")
  gammaSys.actorOf(DB.props(gamma, 2l, RF, CL), "gamma")

  alphaSys.actorOf(KeyValueStorageBackend2.props, DB.PathSegment)
  bettaSys.actorOf(KeyValueStorageBackend2.props, DB.PathSegment)
  gammaSys.actorOf(KeyValueStorageBackend2.props, DB.PathSegment)

  Helpers.wait(20.second)

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

  Helpers.waitForAllNodesUp(alphaSys, bettaSys, gammaSys2)

  Helpers.wait(30.second)

  alpha.leave(alpha.selfAddress)
  alphaSys.terminate

  betta.leave(betta.selfAddress)
  bettaSys.terminate

  /*
  node3Cluster.leave(node3Cluster.selfAddress)
  node3.terminate
  */

  gamma2.leave(gamma2.selfAddress)
  gammaSys2.terminate

}
