package db.cluster

import akka.ConfigurationException
import akka.actor.CoordinatedShutdown.Reason
import akka.cluster.{ Cluster, DowningProvider, Member, MemberStatus }
import akka.actor.{ Actor, ActorLogging, ActorSystem, CoordinatedShutdown, Props, Timers }
import akka.cluster.ClusterEvent.{ ClusterDomainEvent, MemberRemoved, UnreachableMember }

import scala.concurrent.duration.FiniteDuration
import KeepMajoritySbResolver._

object KeepMajoritySbResolver {

  def props(autoDownTimeout: FiniteDuration): Props =
    Props(new KeepMajoritySbResolver(autoDownTimeout))

  case class UnreachableTimeout(member: Member)

  case class UnreachableTimeoutLast2(member: Member)

  case class AttemptAutoDown(member: Member)

  case object ClusterMemberAutoDown extends Reason

  private def majority(n: Int): Int =
    (n + 1) / 2 + (n + 1) % 2

  def isMajority(total: Int, dead: Int): Boolean =
    (total - dead) >= majority(total)
}

class SplitBrainResolver(system: ActorSystem) extends DowningProvider {

  private def clusterSettings = Cluster(system).settings

  override def downRemovalMargin: FiniteDuration =
    clusterSettings.AutoDownUnreachableAfter.asInstanceOf[FiniteDuration]

  override def downingActorProps: Option[Props] =
    clusterSettings.AutoDownUnreachableAfter match {
      case d: FiniteDuration ⇒
        import scala.concurrent.duration._
        Some(KeepMajoritySbResolver.props(d /*.-(1000.millis)*/ ))
      case _ ⇒
        throw new ConfigurationException(
          "KeepMajoritySbResolver downing provider selected but 'akka.cluster.auto-down-unreachable-after' not set"
        )
    }
}

/**
 * Uses the criteria of the majority to autodown nodes avoiding network partition problems.
 * It checks if a node belongs to the majority of the cluster before letting it down an unreachable node.
 * (http://stackoverflow.com/questions/30575174/how-to-configure-downing-in-akka-cluster-when-a-singleton-is-present)
 * The key being is the decision must be the same on both sides but opposite.
 *
 * @autoDownTimeout - Time margin after which shards or singletons that belonged to a downed/removed
 *                  partition are created in surviving partition. The purpose of this margin is that
 *                  in case of a network partition the persistent actors in the non-surviving partitions
 *                  must be stopped before corresponding persistent actors are started somewhere else.
 *                  This is useful if you implement downing strategies that handle network partitions,
 * e.g. by keeping the larger side of the partition and shutting down the smaller side.
 *                  Decision is taken by the strategy when there has been no membership or
 *                  reachability changes for this duration, i.e. the cluster state is stable.
 *
 */
class KeepMajoritySbResolver(autoDownTimeout: FiniteDuration) extends Actor with ActorLogging with Timers {
  var isAutoDowning: Boolean = false
  val Key = "autoDown"

  implicit val ec = context.system.dispatcher

  val cluster = Cluster(context.system)
  val shutdown = CoordinatedShutdown(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[ClusterDomainEvent])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  def exiting: Receive = {
    case _ ⇒ //ignore
  }

  /*
    If only two nodes left in the cluster (a,b) and both are seed nodes and network partitions happens
    then:
      On a we down b
        and
      On b down self

    The idea being here is that we want the first seed node to survive
  */
  def resolveLastTwo: Receive = {
    case KeepMajoritySbResolver.UnreachableTimeoutLast2(member) ⇒
      if (cluster.state.unreachable.contains(member) && !isAutoDowning) {
        val seeds = cluster.settings.SeedNodes
        //none of them is in the seed-nodes
        if (!seeds.contains(cluster.selfUniqueAddress.address) && !seeds.contains(member.uniqueAddress.address)) {
          log.error("ResolveLastTwo(none of them is in the seed-nodes)")
          //keep oldest
          if (Member.addressOrdering.compare(cluster.selfUniqueAddress.address, member.uniqueAddress.address) < 0) {
            log.error("ResolveLastTwo: Keep self, force exit member: {}", member)
            cluster.down(member.address)
            context.become(active)
          } else {
            isAutoDowning = true
            log.error("ResolveLastTwo: Force exit self: {}", cluster.selfAddress)
            shutdown.run(ClusterMemberAutoDown)
            context.become(exiting)
          }
        } else //both in the seed-nodes
        if (seeds.contains(cluster.selfUniqueAddress.address) && seeds.contains(member.uniqueAddress.address)) {
          if (seeds.indexOf(cluster.selfUniqueAddress.address) < seeds.indexOf(member.uniqueAddress.address)) {
            // self 1, member 2 or more -> Keep self, kill the member
            log.error("ResolveLastTwo(Both seeds: self higher): Keep self, force exit {}", member)
            cluster.down(member.address)
            context.become(active)
          } else {
            // self 2, member 1 -> Kill self, keep the member
            log.error(
              "ResolveLastTwo(Both seeds: member higher): Force exit self {}. This probably will stop the last member in the cluster",
              member
            )
            isAutoDowning = true
            log.error("ResolveLastTwo: Force exit self {}", cluster.selfAddress)
            shutdown.run(ClusterMemberAutoDown)
            context.become(exiting)
          }
        } else //self is on the seen nodes list, the member is not
        if (seeds.contains(cluster.selfUniqueAddress.address) && !seeds.contains(member.uniqueAddress.address)) {
          log.error("ResolveLastTwo(Self:seed, member:not => Keep self, kill the member)")
          cluster.down(member.address)
          context.become(active)
        } else //self is not on the seen nodes list, the member is
        if (!seeds.contains(cluster.selfUniqueAddress.address) && seeds.contains(member.uniqueAddress.address)) {
          log.error("ResolveLastTwo(Self:not in the seed, member:is => Force exit self {}", cluster.selfAddress)
          isAutoDowning = true
          shutdown.run(ClusterMemberAutoDown)
          context.become(exiting)
        }
      } else {
        log.error("Force exit avoided because {} is reachable again", member)
        context.become(active)
      }
    case MemberRemoved(member, prev) ⇒
      if (prev == MemberStatus.Exiting)
        log.error("Member {} removed gracefully", member)
      else
        log.error("Member {} auto taken down after being unreachable", member)

    case _: ClusterDomainEvent ⇒ // ignore
  }

  def active: Receive = {
    case UnreachableMember(member) ⇒
      val state = cluster.state
      log.error("{} detected unreachable", member)

      if (state.members.size == 2) {
        timers.startSingleTimer(Key, KeepMajoritySbResolver.UnreachableTimeoutLast2(member), autoDownTimeout)
        log.warning("NP between last 2 nodes {}", member)
        context.become(resolveLastTwo)
        // See if this member is in the majority of the members
      } else if (isMajority(state.members.size, state.unreachable.size)) {
        log.error("{} is in majority", cluster.selfAddress)
        timers.startSingleTimer(Key, KeepMajoritySbResolver.UnreachableTimeout(member), autoDownTimeout)
      } else {
        log.error("{} is in minority", cluster.selfAddress)
        timers.startSingleTimer(Key, KeepMajoritySbResolver.AttemptAutoDown(member), autoDownTimeout)
      }

    case KeepMajoritySbResolver.AttemptAutoDown(member) ⇒
      // Check if the member is still unreachable
      if (cluster.state.unreachable.contains(member) && !isAutoDowning) {
        isAutoDowning = true
        log.error("Force exit self {}", cluster.selfAddress)
        shutdown.run(ClusterMemberAutoDown)
        context.become(exiting)
      } else
        log.error("Force exit avoided because {} is reachable again", member)

    case KeepMajoritySbResolver.UnreachableTimeout(member) ⇒
      // Check if the member is still unreachable
      if (cluster.state.unreachable.nonEmpty) {
        if (cluster.state.unreachable.contains(member)) {
          log.error("Force exit minority {}", member)
          cluster.down(member.address)
        } else {
          log.error("Force exit avoided because {} is reachable again", member)
        }
      } else
        log.error("Unreachable member {} has already been removed", member)

    case MemberRemoved(member, prev) ⇒
      if (prev == MemberStatus.Exiting) {
        log.error("Member {} removed gracefully", member)
      } else {
        log.error("Member {} auto taken down after being unreachable", member)
      }

    case _: ClusterDomainEvent ⇒ // ignore
  }

  override def receive = active
}

