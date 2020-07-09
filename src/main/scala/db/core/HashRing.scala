package db.core

import java.util.concurrent.ThreadLocalRandom

import akka.actor.Address
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.cluster.typed.{Cluster, SelfUp, Unsubscribe}
import akka.util.Timeout
import db.hashing.Rendezvous

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import akka.actor.typed.scaladsl.AskPattern._
import db.core.KeyValueStorageBackend3.{BuySeat, BuySeatResult, KVRequest3}

object HashRing {

  val Name = "replica"

  val keys = Vector("a", "b", "c", "d", "e")

  sealed trait DbReplicaOps

  case object SelfUpDb extends DbReplicaOps

  case class MembershipChanged(replicas: Set[ActorRef[KVRequest3]]) extends DbReplicaOps

  case object WritePulse extends DbReplicaOps

  def apply(rf: Int, consistencyLevel: Int, id: Long): Behavior[DbReplicaOps] =
    Behaviors.setup { ctx ⇒
      ctx.log.info("{} Starting up replica", id)
      val c = Cluster(ctx.system)
      c.subscriptions ! akka.cluster.typed.Subscribe(
        ctx.messageAdapter[SelfUp] { case SelfUp(_) ⇒ SelfUpDb },
        classOf[SelfUp]
      )
      selfUp(c, id, consistencyLevel)
    }

  def selfUp(c: Cluster, id: Long, consistencyLevel: Int): Behavior[DbReplicaOps] =
    Behaviors.receive { (ctx, _) ⇒
      c.subscriptions ! Unsubscribe(ctx.self)

      ctx.system.receptionist ! akka.actor.typed.receptionist.Receptionist.Subscribe(
        KeyValueStorageBackend.serviceKey,
        ctx.messageAdapter[akka.actor.typed.receptionist.Receptionist.Listing] {
          case KeyValueStorageBackend.serviceKey.Listing(replicas) ⇒
            MembershipChanged(replicas)
        }
      )

      Behaviors.withTimers[DbReplicaOps] { timers ⇒
        timers.startSingleTimer(WritePulse, WritePulse, 3000.millis)
        val writeTo: Timeout = Timeout(2.seconds)

        running(
          Rendezvous[Replica],
          TreeMap.empty[Address, ActorRef[KVRequest3]](Address.addressOrdering),
          c.selfMember.address,
          id,
          consistencyLevel
        )(ctx, writeTo)
      }
    }

  def running(
    hash: Rendezvous[Replica],
    av: SortedMap[Address, ActorRef[KVRequest3]],
    selfAddr: Address,
    id: Long,
    consistencyLevel: Int
  )(implicit ctx: ActorContext[DbReplicaOps], to: Timeout): Behavior[DbReplicaOps] =
    Behaviors.receiveMessagePartial {
      case MembershipChanged(rs) ⇒
        //TODO: handle it properly. You need to reconstruct the whole ring for the ground up

        ctx.log.warn("★ ★ ★ {} Cluster changed:{}", id, rs.mkString(","))

        //idempotent add
        rs.foreach { r ⇒
          if (r.path.address.hasLocalScope) hash.add(Replica(selfAddr))
          else hash.add(Replica(r.path.address))
        }

        val replicas = rs.foldLeft(TreeMap.empty[Address, ActorRef[KVRequest3]]) { (acc, ref) ⇒
          if (ref.path.address.host.isEmpty)
            acc + (selfAddr → ref)
          else
            acc + (ref.path.address → ref)
        }
        running(hash, replicas, selfAddr, id, consistencyLevel)

      case WritePulse ⇒
        implicit val ec  = ctx.executionContext
        implicit val sch = ctx.system.scheduler

        val voucher  = keys(ThreadLocalRandom.current.nextInt(1000) % keys.size)
        val replicas = Try(hash.memberFor(voucher, consistencyLevel)).getOrElse(Set.empty)
        val storages = replicas.map(r ⇒ av.get(r.addr)).flatten
        ctx.log.info("{} goes to:[{}]. All replicas:[{}]", voucher, replicas.mkString(","), storages)

        Future
          .traverse(storages.toVector) { dbRef ⇒
            dbRef.ask[BuySeatResult](BuySeat(voucher, System.nanoTime.toString, _))
          }
          .onComplete {
            case Success(_) ⇒
              ctx.self ! WritePulse
            //ctx.scheduleOnce(10.millis, ctx.self, WritePulse)
            case Failure(db.core.txn.InvariantViolation(_)) ⇒
              ctx.scheduleOnce(100.millis, ctx.self, WritePulse)
            case Failure(ex) ⇒
              ctx.log.error("Write error:", ex)
              ctx.scheduleOnce(100.millis, ctx.self, WritePulse)
          }
        Behaviors.same
    }
}
