package db.core

import java.util.concurrent.ThreadLocalRandom

import akka.actor.Address
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.typed.{Cluster, SelfUp, Unsubscribe}
import akka.util.Timeout
import db.core.KeyValueStorageBackend3.{Protocol, ReservationReply, ReserveSeat}
import db.hashing.Rendezvous

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object HashRing {

  val Name = "replica"

  val voucherKeys = Vector("alpha", "betta", "gamma", "delta")

  sealed trait DbReplicaOps

  case object SelfUpDb extends DbReplicaOps

  case class MembershipChanged(replicas: Set[ActorRef[Protocol]]) extends DbReplicaOps

  case object WritePulse extends DbReplicaOps

  def apply(rf: Int, replicaId: Long): Behavior[DbReplicaOps] =
    Behaviors.setup { ctx ⇒
      ctx.log.info("{} Starting up replica", replicaId)
      val c = Cluster(ctx.system)
      c.subscriptions ! akka.cluster.typed.Subscribe(
        ctx.messageAdapter[SelfUp] { case SelfUp(_) ⇒ SelfUpDb },
        classOf[SelfUp]
      )
      selfUp(c, replicaId, rf)
    }

  def selfUp(c: Cluster, replicaId: Long, rf: Int): Behavior[DbReplicaOps] =
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

        running(
          Rendezvous[Replica],
          TreeMap.empty[Address, ActorRef[Protocol]](Address.addressOrdering),
          c.selfMember.address,
          replicaId,
          rf
        )(ctx, Timeout(2.seconds))
      }
    }

  def running(
    hash: Rendezvous[Replica],
    storages: SortedMap[Address, ActorRef[Protocol]],
    selfAddr: Address,
    id: Long,
    replicaId: Int
  )(implicit ctx: ActorContext[DbReplicaOps], to: Timeout): Behavior[DbReplicaOps] =
    Behaviors.receiveMessagePartial {
      case MembershipChanged(rs) ⇒
        //TODO: handle it properly. You need to reconstruct the whole ring for the ground up

        ctx.log.warn("★ ★ ★ {} ClusterMembership:{}", id, rs.mkString(","))

        //idempotent add
        rs.foreach { r ⇒
          if (r.path.address.hasLocalScope) hash.add(Replica(selfAddr))
          else hash.add(Replica(r.path.address))
        }

        val replicas = rs.foldLeft(TreeMap.empty[Address, ActorRef[Protocol]]) { (acc, ref) ⇒
          if (ref.path.address.host.isEmpty)
            acc + (selfAddr → ref)
          else
            acc + (ref.path.address → ref)
        }
        running(hash, replicas, selfAddr, id, replicaId)

      case WritePulse ⇒
        implicit val ec  = ctx.executionContext
        implicit val sch = ctx.system.scheduler

        val voucher            = voucherKeys(ThreadLocalRandom.current.nextInt() % voucherKeys.size)
        val replicas           = Try(hash.memberFor(voucher, replicaId)).getOrElse(Set.empty)
        val storagesForReplica = replicas.map(r ⇒ storages.get(r.addr)).flatten
        ctx.log.info("{} goes to:[{}]. All replicas:[{}]", voucher, replicas.mkString(","), storages)

        Future
          .traverse(storagesForReplica.toVector) { storage ⇒
            storage.ask[ReservationReply](ReserveSeat(voucher, System.nanoTime.toString, _))
          }
          .transform { r ⇒
            r.map { replies ⇒
              println(voucher + ": " + replies.mkString(",")) //look that we've got
            }
            r
          }
          //TODO: what if it succeeds on one replica and fails on another ???
          //TODO: If N concurrent clients hit the same key at the same time, different winners are possible.
          .onComplete {
            case Success(_) ⇒
              ctx.self ! WritePulse
            //ctx.scheduleOnce(10.millis, ctx.self, WritePulse)
            case Failure(db.core.txn.InvariantViolation(msg)) ⇒
              ctx.log.error(s"InvariantViolation: $msg")
              ctx.scheduleOnce(100.millis, ctx.self, WritePulse)
            case Failure(ex) ⇒
              ctx.log.error("Write error:", ex)
              ctx.scheduleOnce(100.millis, ctx.self, WritePulse)
          }
        Behaviors.same
    }
}
