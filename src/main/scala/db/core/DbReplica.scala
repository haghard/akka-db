package db.core

import akka.actor.Address
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ClusterEvent._
import akka.cluster.MemberStatus
import akka.cluster.typed.{ Cluster, SelfUp, Subscribe }
import db.hashing

import scala.collection.immutable.SortedSet

object DbReplica {

  //val serviceKey = akka.actor.typed.receptionist.ServiceKey[WriteOp]("alarm")

  def apply(RF: Int, WC: Int, id: Long): Behavior[ClusterDomainEvent] = {
    Behaviors.setup { ctx ⇒
      ctx.log.info("{} Starting up Writer", id)
      val c = Cluster(ctx.system)
      c.subscriptions ! akka.cluster.typed.Subscribe(ctx.self, classOf[SelfUp])
      join(c, SortedSet[Address](), SortedSet[Address](), hashing.Rendezvous[db.core.Replica], id)
    }
  }

  def join(
    c: Cluster, available: SortedSet[Address], removed: SortedSet[Address],
    hash: hashing.Rendezvous[Replica], id: Long): Behavior[ClusterDomainEvent] =
    Behaviors.receivePartial {
      case (ctx, msg) ⇒
        //Behaviors.receiveMessagePartial { //receive { (ctx, msg) ⇒
        msg match {
          case SelfUp(state) ⇒
            //ctx.asScala.system.settings.config.getStringList("akka.cluster.seed-nodes")
            val am = state.members.filter(_.status == MemberStatus.Up).map(_.address)
            am.foreach(address ⇒ hash.add(Replica(address)))
            ctx.log.warning("{} joined ★ ★ ★  Ring:{}", id, hash.toString)

            c.subscriptions ! Subscribe(ctx.self, classOf[ClusterDomainEvent]) //MemberEvent ClusterDomainEvent
            convergence(am, removed, hash, id)
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
          case _ ⇒
            Behaviors.same
        }
    }

  def awaitForConvergence(
    availableMembers: SortedSet[Address], removedMembers: SortedSet[Address],
    hash: hashing.Rendezvous[Replica], id: Long
  ): Behavior[ClusterDomainEvent] =
    Behaviors.receivePartial {
      case (ctx, msg) ⇒
        msg match {
          case ReachableMember(member) ⇒
            //ctx.log.warning("{} Reachable = {}", id, member.address)
            convergence(availableMembers, removedMembers, hash, id)
          case UnreachableMember(member) ⇒
            //ctx.log.warning("{} Unreachable = {}", id, member.address)
            awaitForConvergence(availableMembers, removedMembers, hash, id)
          case MemberRemoved(member, _) ⇒
            val rm = removedMembers + member.address
            val am = availableMembers - member.address
            /*ctx.log.warning(
            "{} replica {} was taken downed after being unreachable. Continue with unavailable replica:[{}]",
            id, member.address, rm.mkString(","))*/
            convergence(am, rm, hash, id)
          case _ ⇒
            Behaviors.same
        }
    }
}
