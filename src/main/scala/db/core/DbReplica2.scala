package db.core

import java.util.concurrent.ThreadLocalRandom

import akka.actor.{ Address, Scheduler }
import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.cluster.typed.{ Cluster, SelfUp, Subscribe, Unsubscribe }
import akka.util.Timeout
import db.hashing.Rendezvous

import scala.collection.immutable.{ SortedMap, TreeMap }
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }
import akka.actor.typed.scaladsl.AskPattern._
import db.core.KeyValueStorageBackend3.{ CPut3, KVRequest3, KVResponse3 }

object DbReplica2 {

  val Name = "replica"

  val keys = Vector("a", "b", "c", "d", "e")

  sealed trait DbReplicaOps

  case object SelfUpDb extends DbReplicaOps

  case class MembershipChanged(replicas: Set[ActorRef[KVRequest3]]) extends DbReplicaOps

  case object WritePulse extends DbReplicaOps

  def apply(rf: Int, writeConsistency: Int, id: Long): Behavior[DbReplicaOps] = {
    Behaviors.setup { ctx ⇒
      ctx.log.info("{} Starting up replica", id)
      val c = Cluster(ctx.system)
      c.subscriptions ! akka.cluster.typed.Subscribe(ctx.messageAdapter[SelfUp] {
        case SelfUp(_) ⇒ SelfUpDb
      }, classOf[SelfUp])

      up(c, id)
    }
  }

  def up(c: Cluster, id: Long): Behavior[DbReplicaOps] =
    Behaviors.receive { (ctx, _) ⇒
      c.subscriptions ! Unsubscribe(ctx.self)

      ctx.system.receptionist ! akka.actor.typed.receptionist.Receptionist.Subscribe(
        KeyValueStorageBackend.serviceKey,
        ctx.messageAdapter[akka.actor.typed.receptionist.Receptionist.Listing] {
          case KeyValueStorageBackend.serviceKey.Listing(replicas) ⇒
            MembershipChanged(replicas)
        })

      Behaviors.withTimers[DbReplicaOps] { timers ⇒
        timers.startSingleTimer(WritePulse, WritePulse, 3000.millis)

        implicit val sch = ctx.system.scheduler
        implicit val ec = ctx.system.executionContext
        implicit val writeTo: Timeout = Timeout(2.seconds)

        running(ctx, Rendezvous[Replica],
                TreeMap.empty[Address, ActorRef[KVRequest3]](Address.addressOrdering), c.selfMember.address, id)
      }
    }

  def running(ctx: ActorContext[DbReplicaOps], h: Rendezvous[Replica], av: SortedMap[Address, ActorRef[KVRequest3]],
    selfAddr: Address, id: Long)(implicit sch: Scheduler, ec: ExecutionContext, to: Timeout): Behavior[DbReplicaOps] =
    Behaviors.receiveMessage {
      case MembershipChanged(rs) ⇒
        // Need to understand if there are new members to ship the ring
        ctx.log.warning("★ ★ ★ {} Cluster changed:{}", id, rs.mkString(","))

        //idempotent add
        rs.foreach { r ⇒
          if (r.path.address.host.isEmpty) h.add(Replica(selfAddr))
          else h.add(Replica(r.path.address))
        }

        val replicas = rs./:(TreeMap.empty[Address, ActorRef[KVRequest3]]) { (acc, ref) ⇒
          if (ref.path.address.host.isEmpty)
            acc + (selfAddr -> ref)
          else
            acc + (ref.path.address -> ref)
        }

        running(ctx, h, replicas, selfAddr, id)
      case WritePulse ⇒
        val key = keys(ThreadLocalRandom.current.nextInt(1000) % keys.size)
        val replicas = Try(h.shardFor(key, 2)).getOrElse(Set.empty)
        val reachable = replicas.map(r ⇒ av.get(r.addr)).flatten
        ctx.log.info("{} goes to:[{}]  alive:[{}]", key, replicas.mkString(","), reachable)

        Future.traverse(reachable.toVector) { dbRef ⇒
          dbRef.ask[KVResponse3](CPut3(key, System.nanoTime.toString, _))
        }.onComplete {
          case Success(_) ⇒
            ctx.self ! WritePulse
          //ctx.scheduleOnce(10.millis, ctx.self, WritePulse)
          case Failure(db.core.txn.InvariantViolation(_)) ⇒
            ctx.scheduleOnce(100.millis, ctx.self, WritePulse)
          case Failure(ex) ⇒
            ctx.log.error(ex, "Write error:")
            ctx.scheduleOnce(100.millis, ctx.self, WritePulse)
        }

        Behaviors.same
      case SelfUpDb ⇒
        ctx.log.error("Got SelfUpDb in running state !!!")
        Behaviors.stopped
    }
}
