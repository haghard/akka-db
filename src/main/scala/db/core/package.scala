package db

import akka.actor.Address

package object core {

  case class Node(host: String, port: Int)

  case class Replica(addr: Address) extends Comparable[Replica] {
    override def compareTo(other: Replica): Int =
      Address.addressOrdering.compare(addr, other.addr)
  }
}
