package db

import akka.actor.Address
import akka.actor.typed.ActorRef

package object core {

  case class Node(host: String, port: Int)

  case class Replica(addr: Address) extends Comparable[Replica] {
    override def compareTo(other: Replica): Int =
      Address.addressOrdering.compare(addr, other.addr)

    override def toString: String =
      addr.host.flatMap(h ⇒ addr.port.map(p ⇒ s"{$h:${p}}")).getOrElse("")
  }

  sealed trait DBOps

  case object SelfUpDb extends DBOps

  case class ReadDb(key: String) extends DBOps

  case class ReplicasChanged(replicas: Set[ActorRef[DBOps]]) extends DBOps

  case class WriteDb(key: String, value: String) extends DBOps

  case object Pulse extends DBOps
}
