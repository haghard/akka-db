package db.core

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import db.Helpers

import scala.concurrent.duration._

//runMain db.core.Runner
object Runner extends App {

  val systemName = "akka-db"

  val config = ConfigFactory.parseString(
    s"""
       akka {

          db-io {
            type = Dispatcher
            executor = "thread-pool-executor"
            thread-pool-executor {
              fixed-pool-size = 6
            }
            throughput = 1000
          }

          cluster {
            roles = [ db ]

            akka.cluster.jmx.multi-mbeans-in-same-jvm = on

            //This means that the cluster leader member will change the unreachable node status to down automatically after the configured time of unreachability.
            //auto-down-unreachable-after = 1 s

            shutdown-after-unsuccessful-join-seed-nodes = 30s
          }

          actor.provider = cluster

          #remote.netty.tcp.hostname = 127.0.0.1

          remote.artery.enabled = true
          remote.artery.canonical.hostname = 127.0.0.1

       }
      """)

  def portConfig(port: Int) =
    ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port")
  //ConfigFactory.parseString(s"akka.remote.netty.tcp.port = $port")

  /*
    The number of failures that can be tolerated is equal to (Replication factor - 1) /2.
    For example, with 3x replication, one failure can be tolerated; with 5x replication, two failures, and so on.
  */

  val node1 = ActorSystem(systemName, portConfig(2550).withFallback(config).withFallback(ConfigFactory.load()))
  val node2 = ActorSystem(systemName, portConfig(2551).withFallback(config).withFallback(ConfigFactory.load()))
  val node3 = ActorSystem(systemName, portConfig(2552).withFallback(config).withFallback(ConfigFactory.load()))
  //val node4 = ActorSystem(systemName, portConfig(2553).withFallback(config).withFallback(ConfigFactory.load()))

  val node1Cluster = Cluster(node1)
  val node2Cluster = Cluster(node2)
  val node3Cluster = Cluster(node3)
  //val node4Cluster = Cluster(node4)

  node1Cluster.join(node1Cluster.selfAddress)
  node2Cluster.join(node1Cluster.selfAddress)
  node3Cluster.join(node1Cluster.selfAddress)
  //node4Cluster.join(node1Cluster.selfAddress)

  Helpers.waitForAllNodesUp(node1, node2, node3)

  val RF = 3
  val CL = 3

  val ticketNmr = 500

  node1.actorOf(DB.props(node1Cluster, 0l, RF, CL), "alpha")
  node2.actorOf(DB.props(node2Cluster, 0l, RF, CL), "betta")
  node3.actorOf(DB.props(node3Cluster, 0l, RF, CL), "gamma")
  //node4.actorOf(Cassandra.props(node4Cluster, 1.seconds, 300, RF, CL), "delta")

  //
  node1.actorOf(KeyValueStorageBackend2.props, DB.PathSegment)
  node2.actorOf(KeyValueStorageBackend2.props, DB.PathSegment)
  node3.actorOf(KeyValueStorageBackend2.props, DB.PathSegment)
  //node4.actorOf(KeyValueStorageBackend2.props, Cassandra.PathSegment)

  Helpers.wait(30.second)

  /*
  println("****************** Kill node3 *********************")
  node3Cluster.leave(node3Cluster.selfAddress)
  node3.terminate


  Helpers.wait(30.second)

  println("****************** new incarnation of node3 joins the cluster *********************")
  val node31 = ActorSystem(systemName, portConfig(2552).withFallback(config))
  val node31Cluster = Cluster(node31)
  node31Cluster.join(node1Cluster.selfAddress)
  node31.actorOf(Cassandra.props(node31Cluster, 2.seconds, 200, RF, CL), "gamma")
  node31.actorOf(CassandraBackend.props, Cassandra.PathSegment)

  Helpers.waitForAllNodesUp(node1, node2, node31)

  Helpers.wait(30.second)*/

  node1Cluster.leave(node1Cluster.selfAddress)
  node1.terminate

  node2Cluster.leave(node2Cluster.selfAddress)
  node2.terminate

  node3Cluster.leave(node3Cluster.selfAddress)
  node3.terminate

  /*node31Cluster.leave(node31Cluster.selfAddress)
  node31.terminate*/
}