package db.core

import akka.actor.Address
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.cluster.ClusterEvent._
import akka.cluster.MemberStatus
import akka.cluster.typed.{ Cluster, SelfUp, Subscribe }
import db.hashing

import scala.collection.immutable.SortedSet

object DbReplica {

  //val serviceKey = akka.actor.typed.receptionist.ServiceKey[WriteOp]("alarm")

  def apply(RF: Int, WC: Int, id: Long): Behavior[ClusterDomainEvent] = {
    Behaviors.setup { ctx ⇒
      ctx.log.info("Starting up Writer:{}", id)
      val c = Cluster(ctx.system)
      c.subscriptions ! akka.cluster.typed.Subscribe(ctx.self, classOf[SelfUp])
      selfUp(c, SortedSet[Address](), SortedSet[Address](), hashing.Rendezvous[db.core.Replica], id)
    }
  }

  def selfUp(
    c: Cluster, availableMembers: SortedSet[Address], removedMembers: SortedSet[Address],
    hash: hashing.Rendezvous[Replica], id: Long
  ): Behavior[ClusterDomainEvent] =
    Behaviors.receivePartial {
      case (ctx, msg) ⇒
        //Behaviors.receiveMessagePartial { //receive { (ctx, msg) ⇒
        msg match {
          case SelfUp(state) ⇒
            ctx.log.info("{} joined cluster and is up", id)

            val avMembers = state.members.filter(_.status == MemberStatus.Up).map(_.address)
            avMembers.foreach(address ⇒ hash.add(Replica(address)))
            ctx.log.warning("{} ★ ★ ★  Ring:{}", id, hash.toString)

            c.subscriptions ! Subscribe(ctx.self, classOf[ClusterDomainEvent]) //MemberEvent ClusterDomainEvent
            converged(ctx, avMembers, removedMembers, hash, id)
        }
    }

  def converged(
    ctx: ActorContext[ClusterDomainEvent],
    availableMembers: SortedSet[Address], removedMembers: SortedSet[Address],
    hash: hashing.Rendezvous[Replica], id: Long
  ): Behavior[ClusterDomainEvent] =
    Behaviors.receiveMessagePartial {
      case MemberUp(member) ⇒
        hash.add(Replica(member.address))
        val av = availableMembers + member.address
        val unv = removedMembers - member.address
        ctx.log.warning("{} MemberUp ★ ★ ★  Ring:{}", id, hash.toString)
        converged(ctx, av, unv, hash, id)
      case UnreachableMember(member) ⇒
        ctx.system.log.warning("{} UnreachableMember = {}", id, member.address)
        awaitForClusterConvergence(availableMembers, removedMembers, hash, id)
    }

  def awaitForClusterConvergence(
    availableMembers: SortedSet[Address], removedMembers: SortedSet[Address],
    hash: hashing.Rendezvous[Replica], id: Long
  ): Behavior[ClusterDomainEvent] =
    //.receiveMessagePartial {
    Behaviors.receivePartial {
      case (ctx, msg) ⇒
        msg match {
          case ReachableMember(member) ⇒
            ctx.log.warning("{} ReachableMember = {}", id, member.address)
            converged(ctx, availableMembers, removedMembers, hash, id)
          case UnreachableMember(member) ⇒
            ctx.log.warning("{} UnreachableMember = {}", id, member.address)
            awaitForClusterConvergence(availableMembers, removedMembers, hash, id)
          case MemberRemoved(member, prev) ⇒
            if (prev == MemberStatus.Exiting)
              ctx.log.warning("★ ★ ★  {} {} gracefully exited", id, member.address)
            else
              ctx.log.warning("★ ★ ★  {} {} downed after being unreachable", id, member.address)

            converged(ctx, availableMembers - member.address, removedMembers + member.address, hash, id)
          case _ ⇒
            Behaviors.same
        }
    }
}
