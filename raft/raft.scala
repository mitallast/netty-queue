package org.mitallast.raft

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, FSM, Props, Stash}
import akka.util.Timeout
import akka.pattern.ask

import scala.annotation.{switch, tailrec}
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

case class Term(term: Long) extends AnyVal {
  def prev = this - 1

  def next = this + 1

  def -(n: Long): Term = Term(term - n)

  def +(n: Long): Term = Term(term + n)

  def >(otherTerm: Term): Boolean = this.term > otherTerm.term

  def <(otherTerm: Term): Boolean = this.term < otherTerm.term

  def >=(otherTerm: Term): Boolean = this.term >= otherTerm.term

  def <=(otherTerm: Term): Boolean = this.term <= otherTerm.term
}

object State {
  sealed trait RaftState
  case object Init extends RaftState
  case object Follower extends RaftState
  case object Candidate extends RaftState
  case object Leader extends RaftState
}

object StateMetadata {

  case class Meta(
                   currentTerm: Term,
                   config: ClusterConfiguration,
                   votedFor: Option[ActorRef] = None,
                   votesReceived: Int = 0
                 ) {

    def membersWithout(member: ActorRef) = config.members - member

    def members = config.members

    def isConfigTransitionInProgress = config.isTransitioning

    def canVoteIn(term: Term) = votedFor.isEmpty && term == currentTerm

    def cannotVoteIn(term: Term) = term < currentTerm || votedFor.isDefined

    def forNewElection = Meta(currentTerm + 1, config)

    def withTerm(term: Term) = copy(currentTerm = term, votedFor = None)

    def incTerm = copy(currentTerm = currentTerm + 1)

    def withVoteFor(candidate: ActorRef) = copy(votedFor = Some(candidate))

    def withConfig(conf: ClusterConfiguration) = copy(config = conf)

    def hasMajority = votesReceived > config.members.size / 2

    def incVote = copy(votesReceived = votesReceived + 1)

    def forLeader = Meta(currentTerm, config)

    def forFollower(term: Term = currentTerm) = Meta(term, config)
  }

  def initial(implicit self: ActorRef) = Meta(Term(0), StableClusterConfiguration(0, Set.empty), None, 0)
}

object DomainEvent {
  sealed trait DomainEvent
  case class UpdateTermEvent(t: Term) extends DomainEvent
  case class VoteForEvent(voteFor: ActorRef) extends DomainEvent
  case class VoteForSelfEvent() extends DomainEvent
  case class IncrementVoteEvent() extends DomainEvent
  case class GoToFollowerEvent(t: Option[Term] = None) extends DomainEvent
  case class WithNewConfigEvent(t: Option[Term] = None, config: ClusterConfiguration) extends DomainEvent
  case class GoToLeaderEvent() extends DomainEvent
  case class StartElectionEvent() extends DomainEvent
  case class KeepStateEvent() extends DomainEvent
}

object RaftProtocol {

  case class ClientMessage(client: ActorRef, cmd: Any)

  sealed trait ElectionMessage
  case object BeginElection extends ElectionMessage
  case object ElectedAsLeader extends ElectionMessage
  case object ElectionTimeout extends ElectionMessage


  case class RequestVote(term: Term, candidateId: ActorRef, lastLogTerm: Term, lastLogIndex: Int)
  case class VoteCandidate(term: Term) extends ElectionMessage
  case class DeclineCandidate(term: Term) extends ElectionMessage

  case class AppendEntries(term: Term, prevLogTerm: Term, prevLogIndex: Int, entries: immutable.Seq[LogEntry], leaderCommitId: Int)
  case class AppendRejected(term: Term)
  case class AppendSuccessful(term: Term, lastIndex: Int)

  case object InitLogSnapshot
  case class InstallSnapshot(term: Term, snapshot: RaftSnapshot)
  case class InstallSnapshotRejected(term: Term)
  case class InstallSnapshotSuccessful(term: Term, lastIndex: Int)
  case class RaftSnapshotMetadata(lastIncludedTerm: Term, lastIncludedIndex: Int, clusterConfiguration: ClusterConfiguration)
  case class RaftSnapshot(meta: RaftSnapshotMetadata, data: Any) {
    def toEntry = LogEntry(this, meta.lastIncludedTerm, meta.lastIncludedIndex)
  }

  case class LeaderIs(ref: Option[ActorRef], msg: Option[Any])
  case object WhoIsTheLeader

  case object SendHeartbeat
  case object AskForState
  case class IAmInState(state: State.RaftState)

  case class ChangeConfiguration(newConf: ClusterConfiguration)
  case object RequestConfiguration
}

object ClusterProtocol {
  case object RaftMembersDiscoveryTimeout
  case object RaftMembersDiscoveryRequest
  case object RaftMembersDiscoveryResponse
  case class RaftMemberAdded(member: ActorRef, keepInitUntil: Int)
  case class RaftMemberRemoved(member: ActorRef, keepInitUntil: Int)
}

sealed trait ClusterConfiguration {
  def sequenceNumber: Long

  def members: Set[ActorRef]

  def quorum = members.size / 2 + 1

  def isNewer(state: ClusterConfiguration) = sequenceNumber > state.sequenceNumber

  def isTransitioning: Boolean

  def transitionTo(newConfiguration: ClusterConfiguration): ClusterConfiguration

  def transitionToStable: ClusterConfiguration

  def containsOnNewState(member: ActorRef): Boolean
}

case class StableClusterConfiguration(sequenceNumber: Long, members: Set[ActorRef]) extends ClusterConfiguration {
  val isTransitioning = false

  def transitionTo(newConfiguration: ClusterConfiguration): JointConsensusClusterConfiguration =
    JointConsensusClusterConfiguration(sequenceNumber, members, newConfiguration.members)

  def containsOnNewState(ref: ActorRef) = members contains ref

  def transitionToStable = this
}

case class JointConsensusClusterConfiguration(sequenceNumber: Long, oldMembers: Set[ActorRef], newMembers: Set[ActorRef]) extends ClusterConfiguration {
  val members = oldMembers union newMembers

  val isTransitioning = true

  def transitionTo(newConfiguration: ClusterConfiguration) =
    throw new IllegalStateException(s"Cannot start another configuration transition, already in progress! " +
      s"Migrating from [${oldMembers.size}] $oldMembers to [${newMembers.size}] $newMembers")

  def containsOnNewState(member: ActorRef): Boolean = newMembers contains member

  def transitionToStable = StableClusterConfiguration(sequenceNumber + 1, newMembers)
}

case class LogIndexMap private(private var backing: Map[ActorRef, Int]) {

  def decrementFor(member: ActorRef): Int = {
    val value = backing(member) - 1
    backing = backing.updated(member, value)
    value
  }

  def incrementFor(member: ActorRef): Int = {
    val value = backing(member) + 1
    backing = backing.updated(member, value)
    value
  }

  def put(member: ActorRef, value: Int) = {
    backing = backing.updated(member, value)
  }

  def putIfGreater(member: ActorRef, value: Int): Int = backing.get(member) match {
    case None ⇒
      backing = backing.updated(member, value)
      value
    case Some(oldValue) if value > oldValue ⇒
      backing = backing.updated(member, value)
      value
    case Some(oldValue) ⇒
      oldValue
  }

  def consensusForIndex(config: ClusterConfiguration): Option[Int] = config match {
    case StableClusterConfiguration(_, members) =>
      indexOnMajority(members)

    case JointConsensusClusterConfiguration(_, oldMembers, newMembers) =>
      val oldQuorum = indexOnMajority(oldMembers)
      val newQuorum = indexOnMajority(newMembers)

      if (oldQuorum.isEmpty) newQuorum
      else if (newQuorum.isEmpty) oldQuorum
      else Some(math.min(oldQuorum.get, newQuorum.get))
  }

  private def indexOnMajority(include: Set[ActorRef]): Option[Int] = {
    include match {
      case _ if include.isEmpty ⇒
        None
      case _ ⇒
        val index = LogIndexMap.ceiling(include.size, 2) - 1
        val sorted = include.toList.map(backing.get).sorted
        sorted(index)
    }
  }

  def indexFor(member: ActorRef) = backing.get(member)
}

object LogIndexMap {

  def empty = new LogIndexMap(Map.empty)

  def ceiling(numerator: Int, divisor: Int): Int = {
    if (numerator % divisor == 0) numerator / divisor else (numerator / divisor) + 1
  }
}

case class ReplicatedLog(entries: List[LogEntry], committedIndex: Int, start: Int = 0) {

  import RaftProtocol._

  def containsMatchingEntry(otherPrevTerm: Term, otherPrevIndex: Int): Boolean =
    (otherPrevTerm == Term(0) && otherPrevIndex == 0) ||
      (containsEntryAt(otherPrevIndex) && termAt(otherPrevIndex) == otherPrevTerm && lastIndex == otherPrevIndex)

  def lastTerm = entries.lastOption.map(_.term)

  def lastIndex = entries.lastOption.map(_.index).getOrElse(0)

  def prevIndex = (lastIndex: @switch) match {
    case 0 => 0
    case n => n - 1
  }

  def prevTerm = if (entries.size < 2) Term(0) else entries.dropRight(1).last.term

  def nextIndex = entries.lastOption.map(_.index + 1).getOrElse(1)

  def commit(n: Int): ReplicatedLog = copy(committedIndex = n)

  def append(entry: LogEntry, take: Int = entries.length): ReplicatedLog =
    append(List(entry), take)

  def append(entriesToAppend: Seq[LogEntry], take: Int): ReplicatedLog =
    copy(entries = entries.take(take) ++ entriesToAppend)

  def +(entry: LogEntry): ReplicatedLog =
    append(entry)

  def entriesBatchFrom(fromIncluding: Int, howMany: Int = 5): List[LogEntry] = {
    val toSend = entries.slice(fromIncluding, fromIncluding + howMany)
    toSend.headOption match {
      case Some(head) =>
        val batchTerm = head.term
        toSend.takeWhile(_.term == batchTerm) // we only batch commands grouped by their term

      case None =>
        List.empty
    }
  }

  def between(fromIndex: Int, toIndex: Int): List[LogEntry] =
    entries.slice(fromIndex, toIndex)

  def containsEntryAt(index: Int) = entries.exists(_.index == index)

  def termAt(index: Int): Term = {
    if (index <= 0) {
      Term(0)
    }
    else if (!containsEntryAt(index)) {
      throw new IllegalArgumentException(s"Unable to find log entry at index $index.")
    }
    else {
      entries.find(_.index == index).get.term
    }
  }

  def committedEntries = entries.filter(_.index <= committedIndex)

  def notCommittedEntries = entries.slice(committedIndex + 1, entries.length)

  def compactedWith(snapshot: RaftSnapshot): ReplicatedLog = {
    val snapshotHasLaterTerm = snapshot.meta.lastIncludedTerm > lastTerm.getOrElse(Term(0))
    val snapshotHasLaterIndex = snapshot.meta.lastIncludedIndex > lastIndex

    if (snapshotHasLaterTerm && snapshotHasLaterIndex) {
      copy(entries = List(snapshot.toEntry), start = snapshot.meta.lastIncludedIndex)
    } else {
      val entriesWhereSnapshotEntriesDropped = entries dropWhile { e => e.index <= snapshot.meta.lastIncludedIndex }
      copy(entries = snapshot.toEntry :: entriesWhereSnapshotEntriesDropped, start = snapshot.meta.lastIncludedIndex)
    }
  }

  def hasSnapshot = entries.headOption.map(_.command).exists {
    case _: RaftSnapshot ⇒ true
    case _ ⇒ false
  }

  def snapshot = {
    entries.head.command.asInstanceOf[RaftSnapshot]
  }
}

object ReplicatedLog {
  def empty = ReplicatedLog(List.empty, 0)
}

case class LogEntry(command: Any, term: Term, index: Int, client: Option[ActorRef] = None)

abstract class RaftActor extends FSM[State.RaftState, StateMetadata.Meta]
  with SharedBehaviors
  with Follower
  with Candidate
  with Leader
  with ReplicatedStateMachine
  with ActorLogging {

  import DomainEvent._
  import StateMetadata.Meta
  import RaftProtocol._
  import ClusterProtocol._
  import State._

  var replicatedLog = ReplicatedLog.empty

  override def preStart(): Unit = {
    log.info("starting new raft member, will wait for raft cluster configuration")
    self ! RaftMembersDiscoveryTimeout
  }

  startWith(Init, StateMetadata.initial)

  when(Init)(initialBehavior orElse localClusterBehavior)

  when(Follower)(followerBehavior orElse snapshotBehavior orElse clusterManagementBehavior orElse localClusterBehavior)

  when(Candidate)(candidateBehavior orElse snapshotBehavior orElse clusterManagementBehavior orElse localClusterBehavior)

  when(Leader)(leaderBehavior orElse snapshotBehavior orElse clusterManagementBehavior orElse localClusterBehavior)

  onTransition {
    case State.Init -> State.Follower ⇒
      cancelTimer("raft-discovery-timeout")
      resetElectionDeadline()
    case State.Follower -> State.Candidate ⇒
      self ! BeginElection
      resetElectionDeadline()
    case State.Candidate -> State.Leader ⇒
      self ! ElectedAsLeader
      cancelElectionDeadline()
    case State.Leader -> _ ⇒
      stopHeartbeat()
    case _ -> State.Follower ⇒
      resetElectionDeadline()
  }

  onTermination {
    case _ ⇒
      stopHeartbeat()
  }

  initialize()

  def apply(domainEvent: DomainEvent, meta: Meta): Meta = domainEvent match {
    case UpdateTermEvent(term) ⇒ meta.withTerm(term)
    case GoToFollowerEvent(term) => term.fold(meta.forFollower())(t => meta.forFollower(t))
    case GoToLeaderEvent() => meta.forLeader
    case StartElectionEvent() => meta.forNewElection
    case KeepStateEvent() => meta
    case VoteForEvent(candidate) => meta.withVoteFor(candidate)
    case IncrementVoteEvent() => meta.incVote
    case VoteForSelfEvent() => meta.incVote.withVoteFor(self)
    case WithNewConfigEvent(term, config) => term.fold(meta.withConfig(config))(t => meta.withConfig(config).withTerm(t))
  }

  def cancelElectionDeadline() = {
    cancelTimer("raft-election-timeout")
  }

  def resetElectionDeadline() = {
    cancelElectionDeadline()
    val timeout = Random.nextInt(1000) + 2000
    setTimer("raft-election-timeout", ElectionTimeout, timeout.milliseconds)
  }

  def beginElection(meta: Meta) = {
    resetElectionDeadline()
    if (meta.config.members.isEmpty) {
      goto(Follower) using apply(KeepStateEvent(), meta)
    } else {
      goto(Candidate) using apply(StartElectionEvent(), meta)
    }
  }

  def stepDown(meta: Meta, term: Option[Term] = None) = {
    goto(Follower) using apply(GoToFollowerEvent(term), meta)
  }

  def acceptHeartbeat() = {
    resetElectionDeadline()
    stay()
  }

  def isInconsistentTerm(currentTerm: Term, term: Term): Boolean = term < currentTerm

  @inline def follower = sender()

  @inline def candidate = sender()

  @inline def leader = sender()

  @inline def voter = sender()
}

case class RaftClientActor() extends Actor with ActorLogging with Stash {

  import RaftProtocol._
  import ClusterProtocol._
  import context.dispatcher

  case object FindLeader

  var members = Set.empty[ActorRef]
  var leader: Option[ActorRef] = None

  override def preStart() = {
    self ! FindLeader
  }

  override def receive: Receive = {
    case LeaderIs(Some(newLeader), maybeMsg) =>
      log.info("member {} informed that leader is {} (previously assumed: {})", sender().path.name, newLeader.path.name, leader.map(_.path.name))
      leader = Some(newLeader)
      unstashAll()

    case LeaderIs(None, maybeMsg) =>
      log.info("member {} thinks there is no leader currently in the raft cluster", sender().path.name)

    case FindLeader =>
      asyncRefreshMembers()
      randomMember().foreach(_ ! WhoIsTheLeader)
      if (leader.isEmpty) {
        context.system.scheduler.scheduleOnce(1.seconds, self, FindLeader)
      }

    case RaftMembersDiscoveryResponse ⇒
      members += sender()

    case message: ClientMessage =>
      proxyOrStash(message.cmd, message.client)

    case message =>
      proxyOrStash(message, sender())
  }

  def proxyOrStash(message: Any, client: ActorRef) {
    log.info("got: {}", message)
    leader match {
      case Some(lead) =>
        log.info("proxy message {} from {} to currently assumed leader {}.", message, sender().path.name, lead.path.name)
        lead forward ClientMessage(client, message)

      case _ =>
        log.info("no leader in cluster, stashing client message of type {} from {}, will re-send once leader elected.", message.getClass, client.path.name)
        stash()
    }
  }

  def asyncRefreshMembers() {
    context.actorSelection("/user/raft-node-*") ! RaftMembersDiscoveryRequest
  }

  def randomMember(): Option[ActorRef] = {
    if (members.isEmpty) None
    else members.drop(Random.nextInt(members.size)).headOption
  }
}

trait Follower {
  this: RaftActor ⇒

  import State._
  import DomainEvent._
  import StateMetadata.Meta
  import RaftProtocol._

  val followerBehavior: StateFunction = {
    case Event(msg: ClientMessage, meta: Meta) =>
      log.info("follower got {} from client, respond with last Leader that took write from: {}", msg, recentlyContactedByLeader)
      sender() ! LeaderIs(recentlyContactedByLeader, Some(msg))
      stay()

    // election
    case Event(r@RequestVote(term, candidate, lastLogTerm, lastLogIndex), meta: Meta)
      if term > meta.currentTerm ⇒
      log.info("received newer {}, current term is {}", term, meta.currentTerm)
      self forward r
      stay() using apply(UpdateTermEvent(term), meta)

    case Event(RequestVote(term, candidate, lastLogTerm, lastLogIndex), meta: Meta)
      if meta.canVoteIn(term) ⇒
      resetElectionDeadline()
      if (replicatedLog.lastTerm.exists(lastLogTerm < _)) {
        log.warning("rejecting vote for {}, and {}, candidate's lastLogTerm: {} < ours: {}",
          candidate, term, lastLogTerm, replicatedLog.lastTerm.get)
        sender ! DeclineCandidate(meta.currentTerm)
        stay()
      } else if (replicatedLog.lastTerm.contains(lastLogTerm) &&
        lastLogIndex < replicatedLog.lastIndex) {
        log.warning("rejecting vote for {}, and {}, candidate's lastLogIndex: {} < ours: {}",
          candidate, term, lastLogIndex, replicatedLog.lastIndex)
        sender ! DeclineCandidate(meta.currentTerm)
        stay()
      } else {
        log.info("voting for {} in {}", candidate, term)
        sender ! VoteCandidate(meta.currentTerm)
        stay() using apply(VoteForEvent(candidate), meta)
      }

    case Event(RequestVote(term, candidateId, lastLogTerm, lastLogIndex), meta: Meta) if meta.votedFor.isDefined =>
      log.warning("rejecting vote for {}, and {}, currentTerm: {}, already voted for: {}", candidate, term, meta.currentTerm, meta.votedFor.get)
      sender ! DeclineCandidate(meta.currentTerm)
      stay()

    case Event(RequestVote(term, candidateId, lastLogTerm, lastLogIndex), meta: Meta) =>
      log.warning("rejecting vote for {}, and {}, currentTerm: {}, received stale term number {}", candidate, term, meta.currentTerm, term)
      sender ! DeclineCandidate(meta.currentTerm)
      stay()

    case Event(request: AppendEntries, meta: Meta)
      if request.term > meta.currentTerm ⇒
      log.info("received newer {}, current term is {}", request.term, meta.currentTerm)
      self forward request
      stay() using apply(UpdateTermEvent(request.term), meta)

    case Event(request: AppendEntries, meta: Meta) ⇒
      if (!replicatedLog.containsMatchingEntry(request.prevLogTerm, request.prevLogIndex)) {
        log.warning("rejecting write (inconsistent log): {}:{} {} ", request.prevLogTerm, request.prevLogIndex, replicatedLog)
        leader ! AppendRejected(meta.currentTerm)
        stay()
      } else {
        appendEntries(request, meta)
      }

    case Event(request: InstallSnapshot, meta: Meta) if request.term > meta.currentTerm ⇒
      log.info("received newer {}, current term is {}", request.term, meta.currentTerm)
      self forward request
      stay() using apply(UpdateTermEvent(request.term), meta)

    case Event(request: InstallSnapshot, meta: Meta) if request.term < meta.currentTerm ⇒
      log.info("rejecting install snapshot {}, current term is {}", request.term, meta.currentTerm)
      leader ! InstallSnapshotRejected(meta.currentTerm)
      stay()

    case Event(request: InstallSnapshot, meta: Meta) ⇒
      resetElectionDeadline()
      log.info("got snapshot from {}, is for: {}", leader, request.snapshot.meta)

      apply(request)
      replicatedLog = replicatedLog.compactedWith(request.snapshot)

      log.info("response snapshot installed in {} last index {}", meta.currentTerm, replicatedLog.lastIndex)
      leader ! InstallSnapshotSuccessful(meta.currentTerm, replicatedLog.lastIndex)

      stay()

    case Event(ElectionTimeout, meta: Meta) =>
      beginElection(meta)

    case Event(AskForState, _) =>
      sender() ! IAmInState(Follower)
      stay()
  }

  def appendEntries(msg: AppendEntries, meta: Meta): State = {
    if (leaderIsLagging(msg, meta)) {
      log.info("rejecting write (Leader is lagging) of: {} {}", msg, replicatedLog)
      leader ! AppendRejected(meta.currentTerm)
      return stay()
    }

    senderIsCurrentLeader()

    leader ! append(msg.entries, meta)
    replicatedLog = commitUntilLeadersIndex(meta, msg)

    val config = maybeGetNewConfiguration(msg.entries.map(_.command), meta.config)

    acceptHeartbeat() using apply(WithNewConfigEvent(replicatedLog.lastTerm, config), meta)
  }

  def leaderIsLagging(msg: AppendEntries, meta: Meta): Boolean =
    msg.term < meta.currentTerm

  def append(entries: immutable.Seq[LogEntry], meta: Meta): AppendSuccessful = {
    if (entries.nonEmpty) {
      val atIndex = entries.map(_.index).min
      log.debug("executing: replicatedLog = replicatedLog.append({}, {})", entries, atIndex - 1)
      replicatedLog = replicatedLog.append(entries, take = atIndex - 1)
    }
    log.info("response append successful term:{} lastIndex:{}", meta.currentTerm, replicatedLog.lastIndex)
    AppendSuccessful(meta.currentTerm, replicatedLog.lastIndex)
  }

  @tailrec final def maybeGetNewConfiguration(entries: Seq[Any], config: ClusterConfiguration): ClusterConfiguration = entries match {
    case Nil =>
      config

    case (newConfig: ClusterConfiguration) :: moreEntries if newConfig.isNewer(config) =>
      log.info("appended new configuration (seq: {}), will start using it now: {}", newConfig.sequenceNumber, newConfig)
      maybeGetNewConfiguration(moreEntries, config)

    case _ :: moreEntries =>
      maybeGetNewConfiguration(moreEntries, config)
  }

  def commitUntilLeadersIndex(meta: Meta, msg: AppendEntries): ReplicatedLog = {
    val entries = replicatedLog.between(replicatedLog.committedIndex, msg.leaderCommitId)

    entries.foldLeft(replicatedLog) { case (repLog, entry) =>
      log.debug("committing entry {} on follower, leader is committed until [{}]", entry, msg.leaderCommitId)

      handleCommitIfSpecialEntry.applyOrElse(entry, handleNormalEntry)

      repLog.commit(entry.index)
    }
  }

  private def senderIsCurrentLeader(): Unit =
    recentlyContactedByLeader = Some(sender())

  private val handleNormalEntry: PartialFunction[Any, Unit] = {
    case entry: LogEntry ⇒ apply(entry.command)
  }

  private val handleCommitIfSpecialEntry: PartialFunction[Any, Unit] = {
    case LogEntry(jointConfig: ClusterConfiguration, _, _, _) =>
  }
}

trait Candidate {
  this: RaftActor =>

  import State._
  import DomainEvent._
  import StateMetadata.Meta
  import RaftProtocol._

  val candidateBehavior: StateFunction = {
    case Event(msg: ClientMessage, meta: Meta) =>
      log.info("candidate got {} from client, respond with anarchy - there is no leader", msg)
      sender() ! LeaderIs(None, Some(msg))
      stay()

    case Event(BeginElection, meta: Meta) =>
      if (meta.config.members.isEmpty) {
        log.warning("tried to initialize election with no members")
        stepDown(meta)
      } else {
        log.info("initializing election (among {} nodes) for {}", meta.config.members.size, meta.currentTerm)

        val request = RequestVote(meta.currentTerm, self, replicatedLog.lastTerm.getOrElse(Term(0)), replicatedLog.lastIndex)
        meta.membersWithout(self).foreach(_ ! request)
        stay() using apply(VoteForSelfEvent(), meta)
      }

    case Event(msg: RequestVote, meta: Meta) if msg.term < meta.currentTerm =>
      log.info("rejecting request vote msg by {} in {}, received stale {}.", candidate, meta.currentTerm, msg.term)
      candidate ! DeclineCandidate(meta.currentTerm)
      stay()

    case Event(msg: RequestVote, meta: Meta) if msg.term > meta.currentTerm =>
      log.info("received newer {}, current term is {}, revert to follower state.", msg.term, meta.currentTerm)
      stepDown(meta, Some(msg.term))

    case Event(msg: RequestVote, meta: Meta) =>
      if (meta.canVoteIn(msg.term)) {
        log.info("voting for {} in {}", candidate, meta.currentTerm)
        candidate ! VoteCandidate(meta.currentTerm)
        stay() using apply(VoteForEvent(candidate), meta)
      } else {
        log.info("rejecting requestVote msg by {} in {}, already voted for {}", candidate, meta.currentTerm, meta.votedFor.get)
        sender ! DeclineCandidate(meta.currentTerm)
        stay()
      }

    case Event(VoteCandidate(term), meta: Meta) if term < meta.currentTerm =>
      log.info("rejecting vote candidate msg by {} in {}, received stale {}.", voter, meta.currentTerm, term)
      voter ! DeclineCandidate(meta.currentTerm)
      stay()

    case Event(VoteCandidate(term), meta: Meta) if term > meta.currentTerm =>
      log.info("received newer {}, current term is {}, revert to follower state.", term, meta.currentTerm)
      stepDown(meta, Some(term))

    case Event(VoteCandidate(term), meta: Meta) =>
      val votesReceived = meta.votesReceived + 1

      val hasWonElection = votesReceived > meta.config.members.size / 2
      if (hasWonElection) {
        log.info("received vote by {}, won election with {} of {} votes", voter, votesReceived, meta.config.members.size)
        goto(Leader) using apply(GoToLeaderEvent(), meta)
      } else {
        log.info("received vote by {}, have {} of {} votes", voter, votesReceived, meta.config.members.size)
        stay using apply(IncrementVoteEvent(), meta)
      }

    case Event(DeclineCandidate(term), meta: Meta) =>
      if (term > meta.currentTerm) {
        log.info("received newer {}, current term is {}, revert to follower state.", term, meta.currentTerm)
        stepDown(meta, Some(term))
      } else {
        log.info("candidate is declined by {} in term {}", sender(), meta.currentTerm)
        stay()
      }

    case Event(append: AppendEntries, meta: Meta) ⇒
      val leaderIsAhead = append.term >= meta.currentTerm

      if (leaderIsAhead) {
        log.info("reverting to follower, because got append entries from leader in {}, but am in {}", append.term, meta.currentTerm)
        self forward append
        stepDown(meta)
      } else {
        stay()
      }

    case Event(ElectionTimeout, meta: Meta) if meta.config.members.size > 1 =>
      log.info("voting timeout, starting a new election (among {})", meta.config.members.size)
      self ! BeginElection
      stay() using apply(StartElectionEvent(), meta)

    case Event(ElectionTimeout, meta: Meta) =>
      log.info("voting timeout, unable to start election, don't know enough nodes (members: {})...", meta.config.members.size)
      stepDown(meta)

    case Event(AskForState, _) =>
      sender() ! IAmInState(Candidate)
      stay()
  }

}

trait Leader {
  this: RaftActor =>

  import State._
  import DomainEvent._
  import StateMetadata.Meta
  import RaftProtocol._

  var nextIndex = LogIndexMap.empty
  var nextIndexDefault: Int = 0
  var matchIndex = LogIndexMap.empty

  val leaderBehavior: StateFunction = {
    case Event(ElectedAsLeader, meta: Meta) =>
      log.info("became leader for {}", meta.currentTerm)
      initializeLeaderState(meta.config.members)
      startHeartbeat(meta)
      stay()

    case Event(SendHeartbeat, meta: Meta) =>
      sendHeartbeat(meta)
      stay()

    // already won election, but votes may still be coming in
    case Event(_: ElectionMessage, _) =>
      stay()

    // client request
    case Event(ClientMessage(client, cmd: Any), meta: Meta) =>
      log.info("appending command: [{}] from {} to replicated log", cmd, client)

      val entry = LogEntry(cmd, meta.currentTerm, replicatedLog.nextIndex, Some(client))

      log.debug("adding to log: {}", entry)
      replicatedLog = replicatedLog.append(entry)
      matchIndex.put(self, entry.index)

      log.debug("log status = {}", replicatedLog)

      val updated = maybeUpdateConfiguration(meta, entry.command)
      if (updated.config.containsOnNewState(self))
        stay() using apply(KeepStateEvent(), updated)
      else
        stepDown(updated)

    case Event(append: AppendEntries, meta: Meta) if append.term > meta.currentTerm =>
      log.info("leader ({}) got append entries from fresher leader ({}), will step down and the leader will keep being: {}", meta.currentTerm, append.term, sender())
      stepDown(meta)

    case Event(append: AppendEntries, meta: Meta) if append.term <= meta.currentTerm =>
      log.warning("leader ({}) got append entries from rogue leader ({} @ {}), it's not fresher than self, will send entries, to force it to step down.", meta.currentTerm, sender(), append.term)
      sendEntries(sender(), meta)
      stay()

    case Event(AppendRejected(term), meta: Meta) if term > meta.currentTerm =>
      stepDown(meta, Some(term)) // since there seems to be another leader!

    case Event(response: AppendRejected, meta: Meta) if response.term == meta.currentTerm =>
      log.info("follower {} rejected write: {}, back out the first index in this term and retry", follower, response.term)
      if (nextIndex.indexFor(follower).exists(_ > 1)) {
        nextIndex.decrementFor(follower)
      }
      stay()

    case Event(response: AppendSuccessful, meta: Meta) if response.term == meta.currentTerm =>
      assert(response.lastIndex <= replicatedLog.lastIndex)
      if (response.lastIndex > 0) {
        nextIndex.put(follower, response.lastIndex + 1)
      }
      matchIndex.putIfGreater(follower, response.lastIndex)
      replicatedLog = maybeCommitEntry(meta, matchIndex, replicatedLog)
      stay()

    case Event(msg: AppendSuccessful, meta: Meta) =>
      log.warning("unexpected append successful: {} in term:{}", msg, meta.currentTerm)
      stay()

    case Event(request: InstallSnapshot, meta: Meta) if request.term > meta.currentTerm ⇒
      log.info("leader ({}) got install snapshot from fresher leader ({}), will step down and the leader will keep being: {}", meta.currentTerm, request.term, sender())
      self forward request
      stepDown(meta, Some(request.term))

    case Event(request: InstallSnapshot, meta: Meta) if request.term <= meta.currentTerm ⇒
      log.info("rejecting install snapshot {}, current term is {}", request.term, meta.currentTerm)
      log.warning("leader ({}) got install snapshot from rogue leader ({} @ {}), it's not fresher than self, will send entries, to force it to step down.", meta.currentTerm, sender(), request.term)
      sendEntries(sender(), meta)
      stay()

    case Event(response: InstallSnapshotSuccessful, meta: Meta) if response.term == meta.currentTerm ⇒
      assert(response.lastIndex <= replicatedLog.lastIndex)
      if (response.lastIndex > 0) {
        nextIndex.put(follower, response.lastIndex + 1)
      }
      matchIndex.putIfGreater(follower, response.lastIndex)
      replicatedLog = maybeCommitEntry(meta, matchIndex, replicatedLog)
      stay()

    case Event(response: InstallSnapshotSuccessful, meta: Meta) ⇒
      log.warning("unexpected install snapshot successful: {} in term:{}", response, meta.currentTerm)
      stay()

    case Event(response: InstallSnapshotRejected, meta: Meta) if response.term > meta.currentTerm ⇒
      stepDown(meta, Some(response.term)) // since there seems to be another leader!

    case Event(response: InstallSnapshotRejected, meta: Meta) if response.term == meta.currentTerm ⇒
      log.info("follower {} rejected write: {}, back out the first index in this term and retry", follower, response.term)
      if (nextIndex.indexFor(follower).exists(_ > 1)) {
        nextIndex.decrementFor(follower)
      }
      stay()

    case Event(msg: InstallSnapshotRejected, meta: Meta) ⇒
      log.warning("unexpected install snapshot successful: {} in term:{}", msg, meta.currentTerm)
      stay()

    case Event(RequestConfiguration, meta: Meta) =>
      sender() ! ChangeConfiguration(meta.config)
      stay()

    case Event(AskForState, _) =>
      sender() ! IAmInState(Leader)
      stay()
  }

  def initializeLeaderState(members: Set[ActorRef]) {
    nextIndex = LogIndexMap.empty
    nextIndexDefault = replicatedLog.lastIndex
    matchIndex = LogIndexMap.empty
    log.info("prepare next index and match index table for followers: next index:{}", nextIndexDefault)
  }

  def sendEntries(follower: ActorRef, meta: Meta) {
    val lastIndex = nextIndex.indexFor(follower).getOrElse(nextIndexDefault)

    if (replicatedLog.hasSnapshot) {
      val snapshot = replicatedLog.snapshot
      if (snapshot.meta.lastIncludedIndex >= lastIndex) {
        log.info("send install snapshot to {} in term {}", follower.path.name, meta.currentTerm)
        follower ! InstallSnapshot(meta.currentTerm, snapshot)
        return
      }
    }

    log.info("send heartbeat to {} in term {}", follower.path.name, meta.currentTerm)
    follower ! appendEntries(
      meta.currentTerm,
      replicatedLog,
      lastIndex = lastIndex,
      leaderCommitIdx = replicatedLog.committedIndex
    )
  }

  def appendEntries[T](term: Term, replicatedLog: ReplicatedLog, lastIndex: Int, leaderCommitIdx: Int): AppendEntries = {
    val entries: List[LogEntry] = {
      if (lastIndex > replicatedLog.nextIndex) {
        throw new Error(s"unexpected from index $lastIndex > ${replicatedLog.nextIndex}")
      } else {
        replicatedLog.entriesBatchFrom(lastIndex)
      }
    }
    val prevIndex = List(0, lastIndex - 1).max
    val prevTerm = replicatedLog.termAt(prevIndex)
    log.info("send append entries[{}] term:{} from index:{}", entries.size, term, lastIndex)
    AppendEntries(term, prevTerm, prevIndex, entries, leaderCommitIdx)
  }

  def stopHeartbeat() {
    cancelTimer("raft-heartbeat")
  }

  def startHeartbeat(meta: Meta) {
    sendHeartbeat(meta)
    log.info("starting heartbeat")
    setTimer("raft-heartbeat", SendHeartbeat, 1000.milliseconds, repeat = true)
  }

  def sendHeartbeat(meta: Meta) {
    meta.membersWithout(self).foreach(sendEntries(_, meta))
  }

  def maybeCommitEntry(meta: Meta, matchIndex: LogIndexMap, replicatedLog: ReplicatedLog): ReplicatedLog = {
    matchIndex.consensusForIndex(meta.config)
      .filter(_ > replicatedLog.committedIndex)
      .map { indexOnMajority ⇒
        log.info("consensus for persisted index: {}, committed index: {}", indexOnMajority, replicatedLog.committedIndex)
        val entries = replicatedLog.between(replicatedLog.committedIndex, indexOnMajority)
        entries foreach { entry ⇒
          handleCommitIfSpecialEntry.applyOrElse(entry, default = handleNormalEntry)
        }
        replicatedLog.commit(indexOnMajority)
      }
      .getOrElse(replicatedLog)
  }

  private val handleCommitIfSpecialEntry: PartialFunction[LogEntry, Unit] = {
    case LogEntry(jointConfig: JointConsensusClusterConfiguration, _, _, _) =>
      self ! ClientMessage(self, jointConfig.transitionToStable)

    case LogEntry(stableConfig: StableClusterConfiguration, _, _, _) ⇒
      log.info("now on stable configuration")
  }

  private val handleNormalEntry: PartialFunction[Any, Unit] = {
    case entry: LogEntry =>
      log.info("committing log at index: {}", entry.index)
      log.info("applying command[index={}]: {}, will send result to client: {}", entry.index, entry.command, entry.client)
      val result = apply(entry.command)
      entry.client foreach { client ⇒
        client ! result
      }
  }

  def maybeUpdateConfiguration(meta: Meta, command: Any): Meta = command match {
    case newConfig: ClusterConfiguration if newConfig.isNewer(meta.config) =>
      log.info("appended new configuration, will start using it now: {}", newConfig)
      meta.withConfig(newConfig)
    case _ ⇒
      meta
  }
}

trait SharedBehaviors {
  this: RaftActor =>

  import State._
  import DomainEvent._
  import StateMetadata.Meta
  import RaftProtocol._
  import ClusterProtocol._

  import context.dispatcher

  var recentlyContactedByLeader: Option[ActorRef] = None

  def keepInitUntilFound: Int = 3

  lazy val initialBehavior: StateFunction = {

    case Event(ChangeConfiguration(initialConfig), meta: Meta) =>
      log.info("applying initial raft cluster configuration, consists of [{}] nodes: {}",
        initialConfig.members.size,
        initialConfig.members.map(_.path.elements.last).mkString("{", ", ", "}"))

      val deadline = resetElectionDeadline()
      log.info("finished init of new raft member, becoming follower, initial election deadline: {}", deadline)
      goto(Follower) using apply(WithNewConfigEvent(config = initialConfig), meta)

    case Event(msg: AppendEntries, meta: Meta) =>
      log.info("cot append entries from a leader, but am in init state, will ask for it's configuration and join raft cluster")
      leader ! RequestConfiguration
      stay()

    case Event(added: RaftMemberAdded, meta: Meta) =>
      val newMembers = meta.members + added.member
      val initialConfig = StableClusterConfiguration(0, newMembers)
      if (added.keepInitUntil <= newMembers.size) {
        log.info("discovered the required min of {} raft cluster members, becoming follower", added.keepInitUntil)
        goto(Follower) using apply(WithNewConfigEvent(config = initialConfig), meta)
      } else {
        log.info("up to {} discovered raft cluster members, still waiting in init until {} discovered.", newMembers.size, added.keepInitUntil)
        stay() using apply(WithNewConfigEvent(config = initialConfig), meta)
      }

    case Event(removed: RaftMemberRemoved, meta: Meta) =>
      val newMembers = meta.config.members - removed.member
      val waitingConfig = StableClusterConfiguration(0, newMembers)
      log.debug("removed one member, until now discovered {} raft cluster members, still waiting in init until {} discovered.", newMembers.size, removed.keepInitUntil)
      stay() using apply(WithNewConfigEvent(config = waitingConfig), meta)
  }

  lazy val clusterManagementBehavior: StateFunction = {
    case Event(WhoIsTheLeader, meta: Meta) =>
      stateName match {
        case Follower => recentlyContactedByLeader foreach {
          _ forward WhoIsTheLeader
        }
        case Leader => sender() ! LeaderIs(Some(self), None)
        case _ => sender() ! LeaderIs(None, None)
      }
      stay()

    case Event(ChangeConfiguration(newConfiguration), meta: Meta) =>
      val transitioningConfig = meta.config transitionTo newConfiguration
      val configChangeWouldBeNoop =
        transitioningConfig.transitionToStable.members == meta.config.members
      if (configChangeWouldBeNoop) {
      } else {
        log.info("starting transition to new configuration, old [size: {}]: {}, migrating to [size: {}]: {}",
          meta.config.members.size, meta.config.members,
          transitioningConfig.transitionToStable.members.size, transitioningConfig)
        self ! ClientMessage(self, transitioningConfig)
      }
      stay()
  }

  lazy val localClusterBehavior: StateFunction = {
    case Event(RaftMembersDiscoveryTimeout, meta: Meta) ⇒
      val memberSelection = context.actorSelection("/user/raft-node-*")
      setTimer("raft-discovery-timeout", RaftMembersDiscoveryTimeout, 10.seconds)
      memberSelection ! RaftMembersDiscoveryRequest
      stay()

    case Event(RaftMembersDiscoveryRequest, meta: Meta) ⇒
      sender() ! RaftMembersDiscoveryResponse
      stay()

    case Event(RaftMembersDiscoveryResponse, meta: Meta) ⇒
      log.info("adding actor {} to raft cluster", sender())
      self ! RaftMemberAdded(sender(), keepInitUntilFound)
      stay()
  }

  lazy val snapshotBehavior: StateFunction = {
    case Event(InitLogSnapshot, meta: Meta) =>
      val committedIndex = replicatedLog.committedIndex
      val snapshotMeta = RaftSnapshotMetadata(replicatedLog.termAt(committedIndex), committedIndex, meta.config)
      log.info("init snapshot up to: {}:{}", snapshotMeta.lastIncludedIndex, snapshotMeta.lastIncludedTerm)

      val snapshotFuture = prepareSnapshot(snapshotMeta)

      snapshotFuture onSuccess {
        case Some(snapshot) =>
          log.info("successfully prepared snapshot for {}:{}, compacting log now", snapshotMeta.lastIncludedIndex, snapshotMeta.lastIncludedTerm)
          replicatedLog = replicatedLog.compactedWith(snapshot)

        case None =>
          log.info("no snapshot data obtained, skip")
      }

      snapshotFuture onFailure {
        case ex: Throwable =>
          log.error("unable to prepare snapshot!", ex)
      }

      stay()
  }
}

trait ReplicatedStateMachine {

  import RaftProtocol._

  def apply: PartialFunction[Any, Any]

  def prepareSnapshot(snapshotMetadata: RaftSnapshotMetadata): Future[Option[RaftSnapshot]] =
    Future.successful(None)
}

object Raft {

  object WordConcatProtocol {
    case class AppendWord(word: String)
    case object GetWords
  }

  case class WordConcatRaftActor() extends RaftActor {

    import RaftProtocol._
    import WordConcatProtocol._

    var words = List[String]()

    override def keepInitUntilFound: Int = 3

    def apply = {
      case AppendWord(word) =>
        words = words :+ word
        log.info(s"applied command [AppendWord($word)], full words is: $words")
        word

      case GetWords =>
        self ! InitLogSnapshot
        log.info("replying with {}", words)
        words

      case InstallSnapshot(term, snapshot) =>
        log.info("installing snapshot with meta: {}, value: {}", snapshot.meta, snapshot.data)
        words = snapshot.data.asInstanceOf[List[String]]
    }

    override def prepareSnapshot(meta: RaftSnapshotMetadata) = {
      Future.successful(Some(RaftSnapshot(meta, words)))
    }
  }

  def main(args: Array[String]): Unit = {

    import WordConcatProtocol._

    implicit val timeout = Timeout(5.seconds)
    val system = ActorSystem("RaftSystem")

    import system.dispatcher

    system.actorOf(Props[WordConcatRaftActor](), name = "raft-node-1")
    system.actorOf(Props[WordConcatRaftActor](), name = "raft-node-2")
    system.actorOf(Props[WordConcatRaftActor](), name = "raft-node-3")

    val client = system.actorOf(Props[RaftClientActor](), name = "raft-client")

    client ? AppendWord("foo") onComplete { msg ⇒ println(s"complete: $msg") }
    client ? AppendWord("bar") onComplete { msg ⇒ println(s"complete: $msg") }
    client ? GetWords onComplete { msg ⇒ println(s"complete: $msg") }
  }
}
