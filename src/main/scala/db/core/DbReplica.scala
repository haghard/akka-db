
package db.core

import akka.actor.Address
import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.receptionist.Receptionist.{ Listing, Register }
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ClusterEvent._
import akka.cluster.MemberStatus
import akka.cluster.typed.{ Cluster, SelfUp, Subscribe }
import db.hashing

import scala.collection.immutable.SortedSet
import scala.concurrent.duration._

object DbReplica {

  case object ClusterPulse extends ClusterDomainEvent

  def apply(RF: Int, WC: Int, id: Long): Behavior[ClusterDomainEvent] = {
    Behaviors.setup { ctx ⇒
      ctx.log.info("{} Starting up Writer", id)
      val c = Cluster(ctx.system)
      c.subscriptions ! akka.cluster.typed.Subscribe(ctx.self, classOf[SelfUp])

      Behaviors.withTimers[ClusterDomainEvent] { t ⇒
        t.startPeriodicTimer(ClusterPulse, ClusterPulse, 2000.millis)
        join(c, SortedSet[Address](), SortedSet[Address](), hashing.Rendezvous[db.core.Replica], id)
      }
    }
  }

  def join(
    c: Cluster, available: SortedSet[Address], removed: SortedSet[Address],
    hash: hashing.Rendezvous[Replica], id: Long): Behavior[ClusterDomainEvent] =
    Behaviors.receivePartial {
      case (ctx, msg) ⇒
        msg match {
          case SelfUp(state) ⇒
            val am = state.members.filter(_.status == MemberStatus.Up).map(_.address)
            am.foreach(address ⇒ hash.add(Replica(address)))
            ctx.log.warning("{} ★ ★ ★  Ring:{}", id, hash.toString)

            c.subscriptions ! Subscribe(ctx.self, classOf[ClusterDomainEvent])
            convergence(am, removed, hash, id)
          case ClusterPulse ⇒
            ctx.log.info("av:[{}] - rm:[{}]", available.map(_.port.get).mkString(","), removed.map(_.port.get).mkString(","))
            Behaviors.same
          case _ ⇒
            Behaviors.same
        }
    }

  def convergence(
    available: SortedSet[Address], removed: SortedSet[Address],
    hash: hashing.Rendezvous[Replica], id: Long
  ): Behavior[ClusterDomainEvent] =
    //Behaviors.receiveMessagePartial {
    Behaviors.receivePartial {
      case (ctx, msg) ⇒
        msg match {
          case MemberUp(member) ⇒
            hash.add(Replica(member.address))
            val av = available + member.address
            val unv = removed - member.address
            ctx.log.warning("{} ★ ★ ★  Ring:{}", id, hash.toString)
            convergence(av, unv, hash, id)
          case UnreachableMember(member) ⇒
            ctx.system.log.warning("{} Unreachable = {}", id, member.address)
            awaitForConvergence(available, removed, hash, id)
          case MemberExited(member) ⇒ //graceful exit
            val rm = removed + member.address
            val am = available - member.address
            hash.remove(Replica(member.address))
            ctx.log.warning("{} replica {} exit gracefully", id, member.address)
            convergence(am, rm, hash, id)
          case ClusterPulse ⇒
            ctx.log.info("av:[{}] - rm:[{}]", available.map(_.port.get).mkString(","), removed.map(_.port.get).mkString(","))
            Behaviors.same
          case _ ⇒
            Behaviors.same
        }
    }

  def awaitForConvergence(
    available: SortedSet[Address], removed: SortedSet[Address],
    hash: hashing.Rendezvous[Replica], id: Long
  ): Behavior[ClusterDomainEvent] =
    Behaviors.receivePartial {
      case (ctx, msg) ⇒
        msg match {
          case ReachableMember(member) ⇒
            //ctx.log.warning("{} Reachable = {}", id, member.address)
            convergence(available, removed, hash, id)
          case UnreachableMember(member) ⇒
            //ctx.log.warning("{} Unreachable = {}", id, member.address)
            awaitForConvergence(available, removed, hash, id)
          case MemberRemoved(member, _) ⇒
            val rm = removed + member.address
            val am = available - member.address
            /*ctx.log.warning(
            "{} replica {} was taken downed after being unreachable. Continue with unavailable replica:[{}]",
            id, member.address, rm.mkString(","))*/
            convergence(am, rm, hash, id)
          case ClusterPulse ⇒
            ctx.log.info("av:[{}] - rm:[{}]", available.map(_.port.get).mkString(","), removed.map(_.port.get).mkString(","))
            Behaviors.same
          case _ ⇒
            Behaviors.same
        }
    }
}
