package db

import akka.actor.Address

package object core {

  case class Node(host: String, port: Int)

  case class Replica(addr: Address) extends Comparable[Replica] {
    override def compareTo(other: Replica): Int =
      Address.addressOrdering.compare(addr, other.addr)
  }

  /*implicit val nodeOrdering = new scala.Ordering[Node] {
    override def compare(a: Node, b: Node) =
      Ordering.fromLessThan[Node] { (x, y) ⇒
        if (x.host != y.host) x.host.compareTo(y.host) < 0
        else if (x.port != y.port) x.port < y.port
        else false
      }.compare(a, b)
  }*/

  /*case class MVRegister[T](
      values: Map[Node, Set[T]] = Map[Node, Set[T]]().withDefaultValue(Set[T]()),
      versions: VersionVector[Node] = VersionVector.empty[Node](nodeOrdering)) {

    def all =
      values.values.flatten.toSet

    def assign(value: T, node: Node): MVRegister[T] = {
      copy(values + (node -> Set(value)), versions + node)
      //copy(values.updated(node, values(node) + value), versions + node)
    }

    def merge(that: MVRegister[T]): MVRegister[T] = {
      if (versions < that.versions) {
        that
      } else if (versions > that.versions) {
        this
      } else if (versions <> that.versions) {
        println("<>")

        val intersect = values.keySet.intersect(that.values.keySet)

        intersect.

        val union = values.keySet.union(that.values.keySet)

        MVRegister(values ++ that.values, versions merge that.versions)
      } else this //means ==
    }
  }*/

}
