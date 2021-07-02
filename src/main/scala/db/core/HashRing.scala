package db.core

import java.util.concurrent.ThreadLocalRandom

import akka.actor.Address
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.typed.{Cluster, SelfUp, Unsubscribe}
import akka.util.Timeout
import db.core.MVCCStorageBackend.{Buy, ReservationReply}
import db.hashing.Rendezvous

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object HashRing {

  val Name = "replica"

  val voucherKeys = Vector("alpha", "betta", "gamma" /*, "delta"*/ )

  sealed trait Protocol

  case object SelfUpDb extends Protocol

  final case class MembershipChanged(replicas: Set[ActorRef[MVCCStorageBackend.Protocol]]) extends Protocol

  case object Write extends Protocol

  def apply(rf: Int, replicaId: Long): Behavior[Protocol] =
    Behaviors.setup { ctx ⇒
      ctx.log.info("{} Starting up replica", replicaId)
      val c = Cluster(ctx.system)
      c.subscriptions ! akka.cluster.typed
        .Subscribe(ctx.messageAdapter[SelfUp] { case SelfUp(_) ⇒ SelfUpDb }, classOf[SelfUp])
      selfUp(c, replicaId, rf)
    }

  def selfUp(c: Cluster, replicaId: Long, rf: Int): Behavior[Protocol] =
    Behaviors.receive { (ctx, _) ⇒
      c.subscriptions ! Unsubscribe(ctx.self)

      ctx.system.receptionist ! akka.actor.typed.receptionist.Receptionist.Subscribe(
        MVCCStorageBackend.Key,
        ctx.messageAdapter[akka.actor.typed.receptionist.Receptionist.Listing] {
          case MVCCStorageBackend.Key.Listing(replicas) ⇒ MembershipChanged(replicas)
        }
      )

      Behaviors.withTimers[Protocol] { timers ⇒
        timers.startTimerAtFixedRate(Write, 3000.millis)
        //timers.startSingleTimer(Write, Write, 3000.millis)

        running(
          Rendezvous[Replica],
          TreeMap.empty[Address, ActorRef[MVCCStorageBackend.Protocol]](Address.addressOrdering),
          c.selfMember.address,
          replicaId,
          rf
        )(ctx, Timeout(2.seconds))
      }
    }

  def running(
    hash: Rendezvous[Replica],
    storages: SortedMap[Address, ActorRef[MVCCStorageBackend.Protocol]],
    selfAddress: Address,
    replicaId: Long,
    rf: Int
  )(implicit ctx: ActorContext[Protocol], to: Timeout): Behavior[Protocol] =
    Behaviors.receiveMessagePartial {
      case MembershipChanged(rs) ⇒
        //TODO: handle it properly. You need to reconstruct the whole ring from the ground up
        ctx.log.warn("★ ★ ★ {} ClusterMembership:{}", replicaId, rs.mkString(","))

        //idempotent add
        rs.foreach(r ⇒
          if (r.path.address.hasLocalScope) hash.add(Replica(selfAddress)) else hash.add(Replica(r.path.address))
        )

        val replicas = rs.foldLeft(TreeMap.empty[Address, ActorRef[MVCCStorageBackend.Protocol]]) { (acc, ref) ⇒
          if (ref.path.address.hasLocalScope) acc + (selfAddress → ref)
          else acc + (ref.path.address                           → ref)
        }
        running(hash, replicas, selfAddress, replicaId, rf)

      case Write ⇒
        implicit val ec  = ctx.executionContext
        implicit val sch = ctx.system.scheduler

        val voucher           = voucherKeys(ThreadLocalRandom.current.nextInt(0, voucherKeys.size))
        val replicas          = Try(hash.memberFor(voucher, rf)).getOrElse(Set.empty)
        val storageForReplica = replicas.map(r ⇒ storages.get(r.addr)).flatten
        ctx.log.info("{} goes to:[{}]. All replicas:[{}]", voucher, replicas.mkString(","), storages)

        //TODO: this use case is not save because
        // What if it succeeds on one replica and fails on another ???
        // If N concurrent clients hit the same key at the same time on different replicas, different winners are possible.

        Future
          .traverse(storageForReplica.toVector) { replica ⇒
            replica.ask[ReservationReply](Buy(voucher, System.nanoTime.toString, _))
          }

        /*f.transform { r ⇒
          r.map { replies ⇒
            //if ReservationReply.Failure(key, _, replyTo) retry
            //else if ReservationReply.Success next
            //else if ReservationReply.Closed stop
          }
          r
        }*/

        /*ctx.pipeToSelf(f) {
          case Success(reply) =>
            reply match {
              case ReservationReply.Success ⇒
              case ReservationReply.Failure ⇒
              case ReservationReply.Closed ⇒
            }
          case Failure(err) => ???
        }*/

        /*.onComplete {
            case Success(_) ⇒
              ctx.self ! Write
            //ctx.scheduleOnce(10.millis, ctx.self, WritePulse)
            case Failure(db.core.txn.InvariantViolation(msg)) ⇒
              ctx.log.error(s"InvariantViolation: $msg")
              ctx.scheduleOnce(100.millis, ctx.self, Write)
            case Failure(ex) ⇒
              ctx.log.error("Write error:", ex)
              ctx.scheduleOnce(100.millis, ctx.self, Write)
          }*/

        Behaviors.same
    }
}
