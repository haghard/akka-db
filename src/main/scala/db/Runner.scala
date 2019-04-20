package db

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import db.core.{ DB, DbReplica, KeyValueStorageBackend }

import scala.concurrent.duration._
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.typed.{ Join, Leave }

//runMain db.Runner
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

  val RF = 3
  val CL = 3

  val ticketNmr = 500

  import akka.actor.typed.scaladsl.adapter._

  val system1 = akka.actor.typed.ActorSystem(
    DbReplica(RF, CL, 0l),
    systemName, portConfig(2550).withFallback(config).withFallback(ConfigFactory.load()))

  val system2 = akka.actor.typed.ActorSystem(
    DbReplica(RF, CL, 1l),
    systemName, portConfig(2551).withFallback(config).withFallback(ConfigFactory.load()))

  val system3 = akka.actor.typed.ActorSystem(
    DbReplica(RF, CL, 2l),
    systemName, portConfig(2552).withFallback(config).withFallback(ConfigFactory.load()))

  //val node1 = ActorSystem(systemName, portConfig(2550).withFallback(config).withFallback(ConfigFactory.load()))
  //val node2 = ActorSystem(systemName, portConfig(2551).withFallback(config).withFallback(ConfigFactory.load()))
  //val node3 = ActorSystem(systemName, portConfig(2552).withFallback(config).withFallback(ConfigFactory.load()))

  val node1 = akka.cluster.typed.Cluster(system1)
  val node2 = akka.cluster.typed.Cluster(system2)
  val node3 = akka.cluster.typed.Cluster(system3)

  node1.manager ! Join(node1.selfMember.address)
  node2.manager ! Join(node1.selfMember.address)
  node3.manager ! Join(node1.selfMember.address)

  //node1Cluster.join(node1Cluster.selfAddress)
  //node2Cluster.join(node1Cluster.selfAddress)
  //node3Cluster.join(node1Cluster.selfAddress)

  //Helpers.waitForAllNodesUp(system1.toUntyped, system2.toUntyped, system3.toUntyped)
  //Helpers.wait(10.second)

  /*
  node1.actorOf(DB.props(node1Cluster, 0l, RF, CL), "alpha")
  node2.actorOf(DB.props(node2Cluster, 0l, RF, CL), "betta")
  node3.actorOf(DB.props(node3Cluster, 0l, RF, CL), "gamma")
  //node4.actorOf(Cassandra.props(node4Cluster, 1.seconds, 300, RF, CL), "delta")

  //
  node1.actorOf(KeyValueStorageBackend.props, DB.PathSegment)
  node2.actorOf(KeyValueStorageBackend.props, DB.PathSegment)
  node3.actorOf(KeyValueStorageBackend.props, DB.PathSegment)
  //node4.actorOf(KeyValueStorageBackend2.props, Cassandra.PathSegment)

  Helpers.wait(30.second)
  */

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

  /*node3.leave(node3.selfAddress)
  system3.terminate

  Helpers.wait(10.second)

  node1.leave(node1.selfAddress)
  system1.terminate

  node2.leave(node2.selfAddress)
  system2.terminate*/

  Helpers.wait(10.second)

  //node3.manager ! Leave(node3.selfMember.address)
  system3.terminate()

  Helpers.wait(10.second)

  //node2.manager ! Leave(node2.selfMember.address)
  system2.terminate()

  //node1.manager ! Leave(node1.selfMember.address)
  system1.terminate()

}
