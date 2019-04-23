package db.core

import akka.actor.Address
import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.cluster.typed.{ Cluster, SelfUp, Subscribe, Unsubscribe }
import db.hashing.Rendezvous

import scala.collection.immutable.{ SortedMap, TreeMap }
import scala.concurrent.duration._

object DbReplica2 {

  val Name = "replica"

  val serviceKey = ServiceKey[DBOps]("db-ops")

  def apply(rf: Int, writeConsistency: Int, id: Long): Behavior[DBOps] = {
    Behaviors.setup { ctx ⇒
      ctx.log.info("{} Starting up replica", id)
      val c = Cluster(ctx.system)
      c.subscriptions ! akka.cluster.typed.Subscribe(ctx.messageAdapter[SelfUp] {
        case SelfUp(_) ⇒ SelfUpDb
      }, classOf[SelfUp])

      up(c, id)
    }
  }

  def up(c: Cluster, id: Long): Behavior[DBOps] =
    Behaviors.receive { (ctx, _) ⇒
      c.subscriptions ! Unsubscribe(ctx.self)

      ctx.system.receptionist ! akka.actor.typed.receptionist.Receptionist.Subscribe(
        serviceKey,
        ctx.messageAdapter[akka.actor.typed.receptionist.Receptionist.Listing] {
          case serviceKey.Listing(replicas) ⇒
            ReplicasChanged(replicas)
        })
      ctx.system.receptionist ! akka.actor.typed.receptionist.Receptionist.Register(serviceKey, ctx.self)

      Behaviors.withTimers[DBOps] { timers ⇒
        timers.startPeriodicTimer(Pulse, Pulse, 1500.millis)

        running(ctx, Rendezvous[Replica],
          TreeMap.empty[Address, ActorRef[DBOps]](Address.addressOrdering), c.selfMember.address, id)
      }
    }

  def running(ctx: ActorContext[DBOps], h: Rendezvous[Replica],
    av: SortedMap[Address, ActorRef[DBOps]], selfAddr: Address, id: Long): Behavior[DBOps] =
    Behaviors.receiveMessage {
      case WriteDb(k, v) ⇒
        //h.shardFor(k, 2)
        Behaviors.same
      case ReadDb(k) ⇒
        Behaviors.same
      case ReplicasChanged(rs) ⇒
        // Need to understand if there are new members to ship the ring
        ctx.log.warning("★ ★ ★ {} Cluster topology changed:{}", id, rs.mkString(","))

        //idempotent add
        rs.foreach { r ⇒
          if (r.path.address.host.isEmpty) h.add(Replica(selfAddr))
          else h.add(Replica(r.path.address))
        }

        val replicas = rs./:(TreeMap.empty[Address, ActorRef[DBOps]]) { (acc, ref) ⇒
          if (ref.path.address.host.isEmpty)
            acc + (selfAddr -> ref)
          else
            acc + (ref.path.address -> ref)
        }
        running(ctx, h, replicas, selfAddr, id)
      case Pulse ⇒
        ctx.log.info("{} av:[{}] Ring:{}", id, av.keySet.mkString(", "), h.toString)
        Behaviors.same
      case SelfUpDb ⇒
        ctx.log.error("Got SelfUpDb in running state !!!")
        Behaviors.stopped
    }
}
