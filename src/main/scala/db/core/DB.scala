
package db.core

import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.cluster.{ Cluster, MemberStatus }
import akka.cluster.ClusterEvent._
import db.{ Runner, hashing }

import scala.collection.immutable.SortedSet
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }
import DB._
import akka.pattern.ask

import scala.concurrent.duration._

object DB {

  sealed trait KVProtocol

  case class CPut(key: String, value: String, node: Node) extends KVProtocol

  case class CGet(key: String) extends KVProtocol

  sealed trait PutResponse

  case class PutSuccess(key: String, replyTo: ActorRef) extends PutResponse

  case class PutFailure(key: String, th: Throwable, replyTo: ActorRef) extends PutResponse

  sealed trait GetResponse

  case class GetSuccess(value: Option[Set[String]], replyTo: ActorRef) extends GetResponse

  case class GetSuccess0(value: Option[String], replyTo: ActorRef) extends GetResponse

  case class GetFailure(key: String, th: Throwable, replyTo: ActorRef) extends GetResponse

  val PathSegment = "db"

  val StoragePath = s"/user/$PathSegment"

  case object WriteDataTick

  def props(cluster: Cluster, startWith: Long, RF: Int, WC: Int) =
    Props(new DB(cluster, startWith, RF, WC)).withDispatcher("akka.db-io")
}

class DB(cluster: Cluster, startWith: Long, rf: Int, writeC: Int) extends Actor with ActorLogging
  with akka.actor.Timers with Stash {

  val addr = cluster.selfAddress

  val sellCounter = new AtomicInteger(Runner.ticketNmr)

  val keys = Vector("a", "b", "c" /*, "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"*/ )

  implicit val ec = context.dispatcher
  implicit val writeTimeout = akka.util.Timeout(1000.millis)

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  override def preStart = {
    cluster.subscribe(self, classOf[ClusterDomainEvent])
    //timers.startPeriodicTimer("tick" + startWith, WriteDataTick, 150.millis)
    timers.startSingleTimer(WriteDataTick, WriteDataTick, 2.seconds)
  }

  def awaitForConvergence(availableMembers: SortedSet[Address], removedMembers: SortedSet[Address],
    hash: hashing.Rendezvous[Replica], i: Long): Receive = {

    case ReachableMember(member) ⇒
      log.info("ReachableMember = {}", member.address)
      unstashAll()
      context.become(active(availableMembers, removedMembers, hash, i))

    case UnreachableMember(member) ⇒
      log.info("UnreachableMember = {}", member.address)
      context.become(awaitForConvergence(availableMembers, removedMembers, hash, i))

    case MemberRemoved(member, prev) ⇒
      if (prev == MemberStatus.Exiting)
        log.info("{} gracefully exited (autodown)", member.address)
      else
        log.info("{} downed after being unreachable", member.address)

      //hash.remove(Replica(member.address))
      unstashAll()
      context.become(active(availableMembers - member.address, removedMembers + member.address, hash, i))

    case WriteDataTick ⇒
      log.info("stash tick")
      stash()

    case _ ⇒
      stash()
  }

  private def writeEventually(ref: ActorSelection, key: String, value: String): Future[String] =
    (ref ask CPut(key, value, Node(addr.host.get, addr.port.get))).mapTo[PutResponse].flatMap {
      case PutSuccess(v, _) ⇒
        Future.successful(v)
      case PutFailure(_, txn.InvariantViolation(_), _) ⇒
        Future.successful(key)
      case PutFailure(_, _, _) ⇒
        writeEventually(ref, key, value)
    }

  def active(availableMembers: SortedSet[Address], removedMembers: SortedSet[Address], hash: hashing.Rendezvous[Replica], i: Long): Receive = {
    case MemberUp(member) ⇒
      hash.add(Replica(member.address))
      val av = availableMembers + member.address
      val unv = removedMembers - member.address
      log.info("MemberUp = {} av:[{}] unv:[{}]", member.address, av.mkString("-"), unv.mkString("-"))
      context become active(av, unv, hash, i)

    case UnreachableMember(member) ⇒
      log.warning("UnreachableMember = {}", member.address)
      context.system.scheduler.scheduleOnce(200.millis)(self ! WriteDataTick)
      context become awaitForConvergence(availableMembers, removedMembers, hash, i)

    case state: CurrentClusterState ⇒
      val avMembers = state.members.filter(_.status == MemberStatus.Up).map(_.address)
      avMembers.foreach(address ⇒ hash.add(Replica(address)))
      log.info("★ ★ ★  Ring:{}", hash.toString)
      context become active(avMembers, removedMembers, hash, i)

    case WriteDataTick ⇒
      //startWith
      val key = keys(i.toInt % keys.size)
      val replicas: Set[Replica] = Try(hash.memberFor(key.toString, rf)).getOrElse(Set.empty)

      val availableRep = replicas.filter(r ⇒ !removedMembers.exists(_ == r.addr))

      val availableRefs = availableRep
        .map(r ⇒ context.actorSelection(RootActorPath(r.addr) / "user" / PathSegment))

      val unAvailableReplicas = replicas.filter(r ⇒ removedMembers.exists(_ == r.addr))

      log.info("key {} -> [{}] av:[{}]", key.toString, replicas.map(_.addr.port).mkString(";"),
                                         availableRep.map(_.addr.port).mkString(";"))

      //WriteConsistency check
      if (availableRefs.size >= writeC) {
        if (unAvailableReplicas.nonEmpty)
          log.error("{} store hint for:[{}]", key.toString, unAvailableReplicas.map(_.addr).mkString(","))

        Future.traverse(availableRefs.toVector) { ref ⇒
          val value = i.toString
          writeEventually(ref, key, value).flatMap { k ⇒
            (ref ask CGet(k)).mapTo[GetResponse].map {
              case GetSuccess(v, _) ⇒
                v.filter(_.contains(value)).isDefined
              //v.getOrElse(Set.empty[String]).size == 1
              case GetSuccess0(v, _)   ⇒ v
              case GetFailure(_, _, _) ⇒ None
            }
          }
        }.onComplete {
          case Success(rs) ⇒
            if (sellCounter.getAndDecrement >= 0) {
              //rs.filter(_ == false).foreach { _ ⇒
              log.info("{} [{}]", key, rs.mkString(","))
              //}
              context.system.scheduler.scheduleOnce(200.millis)(self ! WriteDataTick)
              //self ! WriteDataTick
            }
          case Failure(ex) ⇒
        }
      } else {
        context.system.scheduler.scheduleOnce(200.millis)(self ! WriteDataTick)
        log.error(s"Couldn't meet cl:{} for write {}", writeC, key)
      }

      context become active(availableMembers, removedMembers, hash, i + 1l)
  }

  override def receive =
    active(SortedSet[Address](), SortedSet[Address](), hashing.Rendezvous[db.core.Replica], startWith)
}
