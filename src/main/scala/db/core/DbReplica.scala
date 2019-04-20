package db.core

import akka.actor.Address
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.cluster.ClusterEvent.{ ClusterDomainEvent, MemberEvent, MemberLeft, MemberRemoved, MemberUp, ReachabilityChanged, ReachabilityEvent, ReachableMember, SeenChanged, UnreachableMember }
import akka.cluster.MemberStatus
import akka.cluster.typed.{ Cluster, SelfUp, Subscribe }
import db.hashing

import scala.collection.immutable.SortedSet

object DbReplica {

  //val serviceKey = akka.actor.typed.receptionist.ServiceKey[WriteOp]("alarm")

  def apply(RF: Int, WC: Int, id: Long): Behavior[ClusterDomainEvent] = {
    //val rf = RF
    //val wc = WC
    //val id = ID
    Behaviors.setup { ctx ⇒
      ctx.log.info("Starting up Writer:{}", id)
      val c = Cluster(ctx.system)
      c.subscriptions ! akka.cluster.typed.Subscribe(ctx.self, classOf[SelfUp])
      selfUp(ctx: ActorContext[ClusterDomainEvent], c, SortedSet[Address](), SortedSet[Address](), hashing.Rendezvous[db.core.Replica], id)
    }
  }

  def selfUp(
    ctx: ActorContext[ClusterDomainEvent], c: Cluster,
    availableMembers: SortedSet[Address], removedMembers: SortedSet[Address],
    hash: hashing.Rendezvous[Replica], id: Long
  ): Behavior[ClusterDomainEvent] =
    Behaviors.receiveMessagePartial { //receive { (ctx, msg) ⇒
      //msg match {
      case SelfUp(state) ⇒
        ctx.log.info("{} joined cluster and is up", id)

        val avMembers = state.members.filter(_.status == MemberStatus.Up).map(_.address)
        avMembers.foreach(address ⇒ hash.add(Replica(address)))
        ctx.log.warning("{} ★ ★ ★  Ring:{}", id, hash.toString)

        c.subscriptions ! Subscribe(ctx.self, classOf[ClusterDomainEvent]) //MemberEvent ClusterDomainEvent
        //c.subscriptions ! Subscribe(ctx.self, classOf[ReachabilityEvent])

        joined(ctx, avMembers, removedMembers, hash, id)
      //}
    }

  def joined(
    ctx: ActorContext[ClusterDomainEvent],
    availableMembers: SortedSet[Address], removedMembers: SortedSet[Address],
    hash: hashing.Rendezvous[Replica], id: Long
  ): Behavior[ClusterDomainEvent] =
    Behaviors.receive[ClusterDomainEvent] { (ctx, msg) ⇒
      msg match {
        case MemberUp(member) ⇒
          hash.add(Replica(member.address))
          val av = availableMembers + member.address
          val unv = removedMembers - member.address
          //ctx.log.warning("{} MemberUp = {} av:[{}] unv:[{}]", id, member.address, av.mkString("-"), unv.mkString("-"))
          ctx.log.warning("{} MemberUp ★ ★ ★  Ring:{}", id, hash.toString)
          joined(ctx, av, unv, hash, id)
        case UnreachableMember(member) ⇒
          ctx.log.warning("{} UnreachableMember = {}", id, member.address)
          Behaviors.same
        /*case MemberRemoved(member, prev) ⇒
          ctx.log.warning("{} MemberRemoved = {}", id, member.address)
          Behaviors.same*/

        case MemberLeft(member) ⇒
          ctx.log.warning("{} MemberLeft = {}", id, member.address)
          joined(ctx, availableMembers - member.address, removedMembers + member.address, hash, id)

        /*case ReachabilityChanged(reachability) ⇒
          ctx.log.warning("{} Unreachable = {}", id, reachability.allUnreachable.map(_.address).mkString(","))
          //awaitForClusterConvergence(ctx, availableMembers, removedMembers, hash, id)
          Behaviors.same*/

        case other ⇒
          ctx.log.debug("{} other = {}", id, other)
          Behaviors.same
        //awaitForClusterConvergence(ctx, availableMembers, removedMembers, hash, id)
      }
    }

  def awaitForClusterConvergence(
    ctx: ActorContext[ClusterDomainEvent],
    availableMembers: SortedSet[Address], removedMembers: SortedSet[Address],
    hash: hashing.Rendezvous[Replica], id: Long
  ): Behavior[ClusterDomainEvent] =
    Behaviors.receiveMessagePartial { //receive { (ctx, msg) ⇒
      //msg match {

      /*case ReachabilityChanged(reachability) ⇒
        Behaviors.same*/

      /*case ReachableMember(member) ⇒
        ctx.log.warning("ReachableMember = {}", member.address)
        joined(ctx, availableMembers, removedMembers, hash, id)

      case UnreachableMember(member) ⇒
        ctx.log.warning("UnreachableMember = {}", member.address)
        awaitForClusterConvergence(ctx, availableMembers, removedMembers, hash, id)
      */

      case MemberRemoved(member, prev) ⇒
        if (prev == MemberStatus.Exiting)
          ctx.log.warning("{} gracefully exited (autodown)", member.address)
        else
          ctx.log.warning("{} downed after being unreachable", member.address)

        //ctx.log.warning("{} MemberUp ★ ★ ★  Ring:{}", id, hash.toString)
        joined(ctx, availableMembers - member.address, removedMembers + member.address, hash, id)
    }

}
