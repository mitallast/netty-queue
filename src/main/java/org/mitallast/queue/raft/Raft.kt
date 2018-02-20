package org.mitallast.queue.raft

import com.google.inject.Inject
import com.typesafe.config.Config
import io.vavr.collection.HashMap
import io.vavr.collection.Map
import io.vavr.collection.Vector
import io.vavr.concurrent.Future
import io.vavr.concurrent.Promise
import io.vavr.control.Option
import org.apache.logging.log4j.CloseableThreadContext
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.common.component.AbstractLifecycleComponent
import org.mitallast.queue.common.events.EventBus
import org.mitallast.queue.raft.RaftState.*
import org.mitallast.queue.raft.cluster.ClusterConfiguration
import org.mitallast.queue.raft.cluster.ClusterDiscovery
import org.mitallast.queue.raft.cluster.StableClusterConfiguration
import org.mitallast.queue.raft.event.MembersChanged
import org.mitallast.queue.raft.persistent.PersistentService
import org.mitallast.queue.raft.persistent.ReplicatedLog
import org.mitallast.queue.raft.protocol.*
import org.mitallast.queue.raft.resource.ResourceRegistry
import org.mitallast.queue.transport.DiscoveryNode
import org.mitallast.queue.transport.TransportController
import org.mitallast.queue.transport.TransportService
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

enum class RaftState {
    Void,
    Follower,
    Candidate,
    Leader
}

class Raft @Inject constructor(
        config: Config,
        private val transportService: TransportService,
        private val transportController: TransportController,
        private val clusterDiscovery: ClusterDiscovery,
        private val persistentService: PersistentService,
        private val registry: ResourceRegistry,
        private val context: RaftContext,
        private val eventBus: EventBus
) : AbstractLifecycleComponent() {
    private val replicatedLog = persistentService.openLog()

    private val bootstrap = config.getBoolean("raft.bootstrap")
    private val electionDeadline = config.getDuration("raft.election-deadline", TimeUnit.MILLISECONDS)
    private val heartbeat = config.getDuration("raft.heartbeat", TimeUnit.MILLISECONDS)
    private val snapshotInterval = config.getInt("raft.snapshot-interval")
    private val maxEntries = config.getInt("raft.max-entries")

    private val stashed = ConcurrentLinkedQueue<ClientMessage>()
    private val sessionCommands = ConcurrentHashMap<Long, Promise<Message>>()
    private val lock: ReentrantLock = ReentrantLock()

    @Volatile private var recentlyContactedByLeader: Option<DiscoveryNode> = Option.none()
    @Volatile private var replicationIndex: Map<DiscoveryNode, Long> = HashMap.empty()
    @Volatile private var nextIndex = LogIndexMap(0)
    @Volatile private var matchIndex = LogIndexMap(0)
    @Volatile private var state: State = VoidState()

    override fun doStart() {
        val meta = RaftMetadata(
                persistentService.currentTerm(),
                StableClusterConfiguration(),
                persistentService.votedFor()
        )
        state = FollowerState(meta).initialize()
    }

    override fun doStop() {
        context.cancelTimer(RaftContext.ELECTION_TIMEOUT)
        context.cancelTimer(RaftContext.SEND_HEARTBEAT)
    }

    override fun doClose() {}

    // fsm related

    fun <T: Message> apply(event: T) {
        lock.lock()
        try {
            CloseableThreadContext.push(state.state().name).use {
                state = when(event) {
                    is AppendEntries -> state.handle(event)
                    is AppendRejected -> state.handle(event)
                    is AppendSuccessful -> state.handle(event)
                    is RequestVote -> state.handle(event)
                    is VoteCandidate -> state.handle(event)
                    is DeclineCandidate -> state.handle(event)
                    is ClientMessage -> state.handle(event)
                    is InstallSnapshot -> state.handle(event)
                    is InstallSnapshotSuccessful -> state.handle(event)
                    is InstallSnapshotRejected -> state.handle(event)
                    is AddServer -> state.handle(event)
                    is AddServerResponse -> state.handle(event)
                    is RemoveServer -> state.handle(event)
                    is RemoveServerResponse -> state.handle(event)
                    else -> state
                }
            }
        } finally {
            lock.unlock()
        }
    }

    fun command(cmd: Message): Future<Message> {
        val promise = Promise.make<Message>()
        var prev: Promise<Message>?
        var session: Long
        do {
            session = ThreadLocalRandom.current().nextLong()
            prev = sessionCommands.putIfAbsent(session, promise)
        } while (prev != null)
        if (logger.isInfoEnabled) {
            logger.info("client command with session {}", session)
        }
        val clientMessage = ClientMessage(
                cmd,
                session
        )
        apply(clientMessage)
        return promise.future()
    }

    fun recentLeader(): Option<DiscoveryNode> {
        return recentlyContactedByLeader
    }

    fun currentState(): RaftState {
        return state.state()
    }

    fun currentMeta(): RaftMetadata {
        return state.meta()
    }

    fun replicatedLog(): ReplicatedLog {
        return replicatedLog
    }

    fun currentStashed(): Vector<Message> {
        return Vector.ofAll(stashed)
    }

    // behavior related

    fun send(node: DiscoveryNode, message: Message) {
        if (node == clusterDiscovery.self) {
            transportController.dispatch(message)
        } else {
            transportService.send(node, message)
        }
    }

    fun senderIsCurrentLeader(leader: DiscoveryNode) {
        if (logger.isDebugEnabled) {
            logger.debug("leader is {}", leader)
        }
        recentlyContactedByLeader = Option.some(leader)
    }

    // additional classes

    private abstract inner class State constructor(private var meta: RaftMetadata) {

        init {
            persistentService.updateState(meta.currentTerm, meta.votedFor)
            state = this
        }

        open fun stay(meta: RaftMetadata): State {
            val prev = this.meta
            this.meta = meta
            persistentService.updateState(meta.currentTerm, meta.votedFor)
            if (prev.config !== meta.config && !meta.config.isTransitioning) {
                eventBus.trigger(MembersChanged(meta.members()))
            }
            return this
        }

        abstract fun state(): RaftState

        fun meta(): RaftMetadata {
            return meta
        }

        // replication

        open fun handle(message: AppendEntries): State = stay(meta())
        open fun handle(message: AppendRejected): State = stay(meta())
        open fun handle(message: AppendSuccessful): State = stay(meta())

        // election

        open fun handle(message: RequestVote): State = stay(meta())
        open fun handle(message: VoteCandidate): State = stay(meta())
        open fun handle(message: DeclineCandidate): State = stay(meta())

        // leader

        open fun handle(message: ClientMessage): State = stay(meta())

        // snapshot

        fun createSnapshot(): State {
            val committedIndex = replicatedLog.committedIndex()
            val snapshotMeta = RaftSnapshotMetadata(replicatedLog.termAt(committedIndex),
                    committedIndex, meta().config)
            if (logger.isInfoEnabled) {
                logger.info("init snapshot up to: {}:{}", snapshotMeta.lastIncludedIndex,
                        snapshotMeta.lastIncludedTerm)
            }

            val snapshot = registry.prepareSnapshot(snapshotMeta)
            if (logger.isInfoEnabled) {
                logger.info("successfully prepared snapshot for {}:{}, compacting log now",
                        snapshotMeta.lastIncludedIndex, snapshotMeta.lastIncludedTerm)
            }
            replicatedLog.compactWith(snapshot)

            return this
        }
        open fun handle(message: InstallSnapshot): State = stay(meta())
        open fun handle(message: InstallSnapshotSuccessful): State = stay(meta())
        open fun handle(message: InstallSnapshotRejected): State = stay(meta())

        // joint consensus

        open fun handle(request: AddServer): State = stay(meta())
        open fun handle(request: AddServerResponse): State = stay(meta())
        open fun handle(request: RemoveServer): State = stay(meta())
        open fun handle(request: RemoveServerResponse): State = stay(meta())

        // stash messages

        fun stash(message: ClientMessage) {
            if (logger.isDebugEnabled) {
                logger.debug("stash {}", message)
            }
            stashed.add(message)
        }
    }

    private inner class VoidState : State(RaftMetadata()) {
        override fun state(): RaftState = Void
        override fun handle(message: AppendEntries): State = stay(meta())
        override fun handle(message: RequestVote): State = stay(meta())
        override fun handle(message: ClientMessage): State = stay(meta())
        override fun handle(message: InstallSnapshot): State = stay(meta())
        override fun handle(request: AddServer): State = stay(meta())
        override fun handle(request: RemoveServer): State = stay(meta())
    }

    private inner class FollowerState(meta: RaftMetadata) : State(meta) {

        override fun state(): RaftState {
            return Follower
        }

        override fun stay(meta: RaftMetadata): FollowerState {
            super.stay(meta)
            return this
        }

        fun gotoCandidate(): State {
            resetElectionDeadline()
            return CandidateState(this.meta().forNewElection()).beginElection()
        }

        fun resetElectionDeadline(): FollowerState {
            if (logger.isDebugEnabled) {
                logger.debug("reset election deadline")
            }
            val timeout = Random().nextInt((electionDeadline / 2).toInt()) + electionDeadline
            context.setTimer(RaftContext.ELECTION_TIMEOUT, timeout) {
                lock.lock()
                try {
                    if (state === this) {
                        state = electionTimeout()
                    } else {
                        throw IllegalStateException()
                    }
                } catch (e: IllegalStateException) {
                    logger.error("error handle election timeout", e)
                } finally {
                    lock.unlock()
                }
            }
            return this
        }

        fun initialize(): State {
            resetElectionDeadline()
            if (replicatedLog.isEmpty) {
                return if (bootstrap) {
                    if (logger.isInfoEnabled) {
                        logger.info("bootstrap cluster")
                    }
                    handle(AddServer(clusterDiscovery.self))
                } else {
                    if (logger.isInfoEnabled) {
                        logger.info("joint cluster")
                    }
                    electionTimeout()
                }
            } else {
                val config = replicatedLog.entries()
                        .map<Message> { it.command }
                        .filter { cmd -> cmd is ClusterConfiguration }
                        .map { cmd -> cmd as ClusterConfiguration }
                        .reduceOption { _, b -> b }
                        .getOrElse(meta().config)

                val meta = meta().withConfig(config).withTerm(replicatedLog.lastTerm())
                return stay(meta)
            }
        }

        override fun handle(message: ClientMessage): State {
            if (recentlyContactedByLeader.isDefined) {
                send(recentlyContactedByLeader.get(), message)
            } else {
                stash(message)
            }
            return this
        }

        override fun handle(message: RequestVote): State {
            var meta = meta()
            if (message.term > meta.currentTerm) {
                if (logger.isInfoEnabled) {
                    logger.info("received newer {}, current term is {}", message.term, meta.currentTerm)
                }
                meta = meta.withTerm(message.term)
            }
            when {
                meta.canVoteIn(message.term) -> {
                    resetElectionDeadline()
                    if (replicatedLog.lastTerm().exists { term -> message.lastLogTerm < term }) {
                        logger.warn("rejecting vote for {} at term {}, candidate's lastLogTerm: {} < ours: {}",
                                message.candidate,
                                message.term,
                                message.lastLogTerm,
                                replicatedLog.lastTerm())
                        send(message.candidate, DeclineCandidate(clusterDiscovery.self, meta.currentTerm))
                        return stay(meta)
                    }
                    if (replicatedLog.lastTerm().exists { term -> term == message.lastLogTerm } && message.lastLogIndex < replicatedLog.lastIndex()) {
                        logger.warn("rejecting vote for {} at term {}, candidate's lastLogIndex: {} < ours: {}",
                                message.candidate,
                                message.term,
                                message.lastLogIndex,
                                replicatedLog.lastIndex())
                        send(message.candidate, DeclineCandidate(clusterDiscovery.self, meta.currentTerm))
                        return stay(meta)
                    }

                    if (logger.isInfoEnabled) {
                        logger.info("voting for {} in {}", message.candidate, message.term)
                    }
                    send(message.candidate, VoteCandidate(clusterDiscovery.self, meta.currentTerm))
                    return stay(meta.withVoteFor(message.candidate))
                }
                meta.votedFor.isDefined -> {
                    logger.warn("rejecting vote for {}, and {}, currentTerm: {}, already voted for: {}",
                            message.candidate,
                            message.term,
                            meta.currentTerm,
                            meta.votedFor)
                    send(message.candidate, DeclineCandidate(clusterDiscovery.self, meta.currentTerm))
                    return stay(meta)
                }
                else -> {
                    logger.warn("rejecting vote for {}, and {}, currentTerm: {}, received stale term number {}",
                            message.candidate, message.term,
                            meta.currentTerm, message.term)
                    send(message.candidate, DeclineCandidate(clusterDiscovery.self, meta.currentTerm))
                    return stay(meta)
                }
            }
        }

        override fun handle(message: AppendEntries): State {
            var meta = meta()
            if (message.term > meta.currentTerm) {
                if (logger.isInfoEnabled) {
                    logger.info("received newer {}, current term is {}", message.term, meta.currentTerm)
                }
                meta = meta.withTerm(message.term)
            }
            // 1) Reply false if term < currentTerm (5.1)
            if (message.term < meta.currentTerm) {
                logger.warn("rejecting write (old term): {} < {} ", message.term, meta.currentTerm)
                send(message.member, AppendRejected(clusterDiscovery.self, meta.currentTerm,
                        replicatedLog.lastIndex()))
                return stay(meta)
            }

            try {
                // 2) Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (5.3)
                return if (!replicatedLog.containsMatchingEntry(message.prevLogTerm, message.prevLogIndex)) {
                    logger.warn("rejecting write (inconsistent log): {}:{} {} ",
                            message.prevLogTerm, message.prevLogIndex,
                            replicatedLog)
                    send(message.member, AppendRejected(clusterDiscovery.self, meta.currentTerm,
                            replicatedLog.lastIndex()))
                    stay(meta)
                } else {
                    appendEntries(message, meta)
                }
            } finally {
                resetElectionDeadline()
            }
        }

        fun appendEntries(msg: AppendEntries, currentMeta: RaftMetadata): State {
            var meta = currentMeta
            senderIsCurrentLeader(msg.member)

            if (!msg.entries.isEmpty) {
                // If an existing entry conflicts with a new one (same index
                // but different terms), delete the existing entry and all that
                // follow it (5.3)

                // Append any new entries not already in the log

                if (logger.isDebugEnabled) {
                    logger.debug("append({})", msg.entries)
                }
                replicatedLog.append(msg.entries)
            }
            if (logger.isDebugEnabled) {
                logger.debug("response append successful term:{} lastIndex:{}", meta.currentTerm, replicatedLog.lastIndex())
            }
            val response = AppendSuccessful(clusterDiscovery.self, meta.currentTerm, replicatedLog.lastIndex())
            send(msg.member, response)

            // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

            if (msg.leaderCommit > replicatedLog.committedIndex()) {
                val entries = replicatedLog.slice(replicatedLog.committedIndex() + 1, msg.leaderCommit)
                for (entry in entries) {
                    if (entry.command is ClusterConfiguration) {
                        if (logger.isInfoEnabled) {
                            logger.info("apply new configuration: {}", entry.command)
                        }
                        meta = meta.withConfig(entry.command)
                    } else if (entry.command is Noop) {
                        if (logger.isTraceEnabled) {
                            logger.trace("ignore noop entry")
                        }
                    } else if (entry.command is RaftSnapshot) {
                        logger.warn("unexpected raft snapshot in log")
                    } else {
                        if (logger.isDebugEnabled) {
                            logger.debug("committing entry {} on follower, leader is committed until [{}]", entry, msg.leaderCommit)
                        }
                        registry.apply(entry.index, entry.command).forEach { result ->
                            if (logger.isDebugEnabled) {
                                logger.debug("success client command session {}", entry.session)
                            }
                            val promise = sessionCommands.remove(entry.session)
                            promise?.success(result)
                        }
                    }
                    replicatedLog.commit(entry.index)
                }
            }

            val config = msg.entries
                    .map<Message> { it.command }
                    .filter { cmd -> cmd is ClusterConfiguration }
                    .map { cmd -> cmd as ClusterConfiguration }
                    .reduceOption { _, b -> b }
                    .getOrElse(meta.config)

            val newState = stay(meta.withTerm(replicatedLog.lastTerm()).withConfig(config))
            newState.unstash()
            return if (replicatedLog.committedEntries() >= snapshotInterval) {
                newState.createSnapshot()
            } else {
                newState
            }
        }

        fun electionTimeout(): State {
            resetElectionDeadline()
            return if (meta().config.members.isEmpty) {
                if (logger.isInfoEnabled) {
                    logger.info("no members found, joint timeout")
                }
                clusterDiscovery.discoveryNodes
                        .filter { it != clusterDiscovery.self }
                        .forEach { send(it, AddServer(clusterDiscovery.self)) }
                this
            } else {
                gotoCandidate()
            }
        }

        // joint consensus

        override fun handle(request: AddServer): State {
            if (bootstrap && meta().config.members.isEmpty &&
                    request.member == clusterDiscovery.self &&
                    replicatedLog.isEmpty) {
                context.cancelTimer(RaftContext.ELECTION_TIMEOUT)
                return LeaderState(this.meta()).selfJoin()
            }
            send(request.member, AddServerResponse(
                    AddServerResponse.Status.NOT_LEADER,
                    recentlyContactedByLeader
            ))
            return this
        }

        override fun handle(request: AddServerResponse): State {
            if (request.status == AddServerResponse.Status.OK) {
                if (logger.isInfoEnabled) {
                    logger.info("successful joined")
                }
            }
            val leader = request.leader
            if (leader.isDefined) {
                senderIsCurrentLeader(leader.get())
                resetElectionDeadline()
            }
            return this
        }

        override fun handle(request: RemoveServer): State {
            send(request.member, RemoveServerResponse(
                    RemoveServerResponse.Status.NOT_LEADER,
                    recentlyContactedByLeader
            ))
            return this
        }

        override fun handle(request: RemoveServerResponse): State {
            if (request.status == RemoveServerResponse.Status.OK) {
                if (logger.isInfoEnabled) {
                    logger.info("successful removed")
                }
            }
            recentlyContactedByLeader = request.leader
            resetElectionDeadline()
            return this
        }

        override fun handle(message: InstallSnapshot): State {
            var meta = meta()
            if (message.term > meta.currentTerm) {
                if (logger.isInfoEnabled) {
                    logger.info("received newer {}, current term is {}", message.term, meta.currentTerm)
                }
                meta = meta.withTerm(message.term)
            }
            if (message.term < meta.currentTerm) {
                if (logger.isInfoEnabled) {
                    logger.info("rejecting install snapshot {}, current term is {}", message.term, meta.currentTerm)
                }
                send(message.leader, InstallSnapshotRejected(clusterDiscovery.self, meta.currentTerm))
                return stay(meta)
            } else {
                resetElectionDeadline()
                if (logger.isInfoEnabled) {
                    logger.info("got snapshot from {}, is for: {}", message.leader, message.snapshot.meta)
                }

                meta = meta.withConfig(message.snapshot.meta.config)
                replicatedLog.compactWith(message.snapshot)
                for (msg in message.snapshot.data) {
                    registry.apply(message.snapshot.meta.lastIncludedIndex, msg)
                }

                if (logger.isInfoEnabled) {
                    logger.info("response snapshot installed in {} last index {}", meta.currentTerm,
                            replicatedLog.lastIndex())
                }
                send(message.leader, InstallSnapshotSuccessful(clusterDiscovery.self,
                        meta.currentTerm, replicatedLog.lastIndex()))

                return stay(meta)
            }
        }

        fun unstash() {
            if (recentlyContactedByLeader.isDefined) {
                val leader = recentlyContactedByLeader.get()
                while (stashed.isNotEmpty()) {
                    send(leader, stashed.poll())
                }
            } else {
                logger.warn("try unstash without leader")
            }
        }
    }

    private inner class CandidateState(meta: RaftMetadata) : State(meta) {

        override fun state(): RaftState {
            return Candidate
        }

        override fun stay(meta: RaftMetadata): CandidateState {
            super.stay(meta)
            return this
        }

        fun gotoFollower(): FollowerState {
            return FollowerState(this.meta().forFollower()).resetElectionDeadline()
        }

        fun gotoLeader(): State {
            context.cancelTimer(RaftContext.ELECTION_TIMEOUT)
            return LeaderState(this.meta()).elected()
        }

        override fun handle(message: ClientMessage): State {
            stash(message)
            return this
        }

        override fun handle(request: AddServer): State {
            send(request.member, AddServerResponse(
                    AddServerResponse.Status.NOT_LEADER,
                    Option.none()
            ))
            return this
        }

        override fun handle(request: RemoveServer): State {
            send(request.member, RemoveServerResponse(
                    RemoveServerResponse.Status.NOT_LEADER,
                    Option.none()
            ))
            return this
        }

        fun resetElectionDeadline() {
            if (logger.isDebugEnabled) {
                logger.debug("reset election deadline")
            }
            val timeout = Random().nextInt((electionDeadline / 2).toInt()) + electionDeadline
            context.setTimer(RaftContext.ELECTION_TIMEOUT, timeout) {
                lock.lock()
                try {
                    if (state === this) {
                        state = electionTimeout()
                    } else {
                        throw IllegalStateException()
                    }
                } catch (e: IllegalStateException) {
                    logger.error("error handle election timeout", e)
                } finally {
                    lock.unlock()
                }
            }
        }

        fun beginElection(): State {
            resetElectionDeadline()
            var meta = meta()
            if (logger.isInfoEnabled) {
                logger.info("initializing election (among {} nodes) for {}", meta.config.members.size(), meta.currentTerm)
            }
            val request = RequestVote(meta.currentTerm, clusterDiscovery.self,
                    replicatedLog.lastTerm().getOrElse(0L), replicatedLog.lastIndex())
            for (member in meta.membersWithout(clusterDiscovery.self)) {
                if (logger.isInfoEnabled) {
                    logger.info("send request vote to {}", member)
                }
                send(member, request)
            }
            meta = meta.incVote().withVoteFor(clusterDiscovery.self)
            return if (meta.hasMajority()) {
                if (logger.isInfoEnabled) {
                    logger.info("received vote by {}, won election with {} of {} votes", clusterDiscovery.self,
                            meta.votesReceived, meta.config.members.size())
                }
                stay(meta).gotoLeader()
            } else {
                stay(meta)
            }
        }

        override fun handle(message: RequestVote): State {
            if (message.term < meta().currentTerm) {
                if (logger.isInfoEnabled) {
                    logger.info("rejecting request vote msg by {} in {}, received stale {}.", message.candidate,
                            meta().currentTerm, message.term)
                }
                send(message.candidate, DeclineCandidate(clusterDiscovery.self, meta().currentTerm))
                return this
            }
            if (message.term > meta().currentTerm) {
                if (logger.isInfoEnabled) {
                    logger.info("received newer {}, current term is {}, revert to follower state.", message.term,
                            meta().currentTerm)
                }
                return stay(meta().withTerm(message.term)).gotoFollower().handle(message)
            }
            if (logger.isInfoEnabled) {
                logger.info("rejecting requestVote msg by {} in {}, already voted for {}", message.candidate,
                        meta().currentTerm, meta().votedFor)
            }
            send(message.candidate, DeclineCandidate(clusterDiscovery.self, meta().currentTerm))
            return this
        }

        override fun handle(message: VoteCandidate): State {
            var meta = meta()
            if (message.term < meta.currentTerm) {
                if (logger.isInfoEnabled) {
                    logger.info("ignore vote candidate msg by {} in {}, received stale {}.", message.member,
                            meta.currentTerm, message.term)
                }
                return this
            }
            if (message.term > meta.currentTerm) {
                if (logger.isInfoEnabled) {
                    logger.info("received newer {}, current term is {}, revert to follower state.", message.term,
                            meta.currentTerm)
                }
                return stay(meta.withTerm(message.term)).gotoFollower()
            }

            meta = meta.incVote()
            return if (meta.hasMajority()) {
                if (logger.isInfoEnabled) {
                    logger.info("received vote by {}, won election with {} of {} votes", message.member,
                            meta.votesReceived, meta.config.members.size())
                }
                stay(meta).gotoLeader()
            } else {
                if (logger.isInfoEnabled) {
                    logger.info("received vote by {}, have {} of {} votes", message.member, meta.votesReceived,
                            meta.config.members.size())
                }
                stay(meta)
            }
        }

        override fun handle(message: DeclineCandidate): State {
            return if (message.term > meta().currentTerm) {
                if (logger.isInfoEnabled) {
                    logger.info("received newer {}, current term is {}, revert to follower state.", message.term,
                            meta().currentTerm)
                }
                stay(meta().withTerm(message.term)).gotoFollower()
            } else {
                if (logger.isInfoEnabled) {
                    logger.info("candidate is declined by {} in term {}", message.member, meta().currentTerm)
                }
                this
            }
        }

        override fun handle(message: AppendEntries): State {
            val leaderIsAhead = message.term >= meta().currentTerm
            return if (leaderIsAhead) {
                if (logger.isInfoEnabled) {
                    logger.info("reverting to follower, because got append entries from leader in {}, but am in {}",
                            message.term, meta().currentTerm)
                }
                stay(meta().withTerm(message.term)).gotoFollower().handle(message)
            } else {
                this
            }
        }

        override fun handle(message: InstallSnapshot): State {
            val leaderIsAhead = message.term >= meta().currentTerm
            return if (leaderIsAhead) {
                if (logger.isInfoEnabled) {
                    logger.info("reverting to follower, because got install snapshot from leader in {}, but am in {}",
                            message.term, meta().currentTerm)
                }
                stay(meta().withTerm(message.term)).gotoFollower().handle(message)
            } else {
                send(message.leader, InstallSnapshotRejected(clusterDiscovery.self, meta().currentTerm))
                this
            }
        }

        fun electionTimeout(): State {
            if (logger.isInfoEnabled) {
                logger.info("voting timeout, starting a new election (among {})", meta().config.members.size())
            }
            return stay(meta().forNewElection()).beginElection()
        }
    }

    private inner class LeaderState(meta: RaftMetadata) : State(meta) {

        override fun state(): RaftState {
            return Leader
        }

        override fun stay(meta: RaftMetadata): LeaderState {
            super.stay(meta)
            return this
        }

        fun gotoFollower(): State {
            context.cancelTimer(RaftContext.SEND_HEARTBEAT)
            return FollowerState(this.meta().forFollower()).resetElectionDeadline()
        }

        fun selfJoin(): State {
            if (logger.isInfoEnabled) {
                logger.info("bootstrap cluster with {}", clusterDiscovery.self)
            }
            val meta = meta()
                    .withTerm(meta().currentTerm + 1)
                    .withConfig(StableClusterConfiguration(clusterDiscovery.self))

            nextIndex = LogIndexMap(replicatedLog.lastIndex() + 1)
            matchIndex = LogIndexMap(0)
            replicationIndex = HashMap.empty()
            val entry = if (replicatedLog.isEmpty) {
                LogEntry(meta.currentTerm, replicatedLog.nextIndex(), 0, meta.config)
            } else {
                LogEntry(meta.currentTerm, replicatedLog.nextIndex(), 0, Noop.INSTANCE)
            }

            replicatedLog.append(entry)
            matchIndex.put(clusterDiscovery.self, entry.index)

            sendHeartbeat()
            startHeartbeat()

            while (stashed.isNotEmpty()) {
                val clientMessage = stashed.poll()
                if (logger.isDebugEnabled) {
                    logger.debug("appending command: [{}] to replicated log", clientMessage.command)
                }
                val logEntry = LogEntry(meta.currentTerm, replicatedLog.nextIndex(), clientMessage.session, clientMessage.command)
                replicatedLog.append(logEntry)
                matchIndex.put(clusterDiscovery.self, entry.index)
            }

            return stay(meta).maybeCommitEntry()
        }

        fun elected(): State {
            if (logger.isInfoEnabled) {
                logger.info("became leader for {}", meta().currentTerm)
            }

            // for each server, index of the next log entry
            // to send to that server (initialized to leader
            // last log index + 1)
            nextIndex = LogIndexMap(replicatedLog.lastIndex() + 1)

            // for each server, index of highest log entry
            // known to be replicated on server
            // (initialized to 0, increases monotonically)
            matchIndex = LogIndexMap(0)

            // for each server store last send heartbeat time
            // 0 if no response is expected
            replicationIndex = HashMap.empty()

            val entry = if (replicatedLog.isEmpty) {
                LogEntry(meta().currentTerm, replicatedLog.nextIndex(), 0, meta().config)
            } else {
                LogEntry(meta().currentTerm, replicatedLog.nextIndex(), 0, Noop.INSTANCE)
            }

            replicatedLog.append(entry)
            matchIndex.put(clusterDiscovery.self, entry.index)

            sendHeartbeat()
            startHeartbeat()

            while (stashed.isNotEmpty()) {
                val clientMessage = stashed.poll()
                if (logger.isDebugEnabled) {
                    logger.debug("appending command: [{}] to replicated log", clientMessage.command)
                }
                val logEntry = LogEntry(meta().currentTerm, replicatedLog.nextIndex(),
                        clientMessage.session, clientMessage.command)
                replicatedLog.append(logEntry)
                matchIndex.put(clusterDiscovery.self, entry.index)
            }

            return maybeCommitEntry()
        }

        override fun handle(message: ClientMessage): State {
            if (logger.isDebugEnabled) {
                logger.debug("appending command: [{}] to replicated log", message.command)
            }
            val entry = LogEntry(meta().currentTerm, replicatedLog.nextIndex(), message.session, message.command)
            replicatedLog.append(entry)
            matchIndex.put(clusterDiscovery.self, entry.index)
            sendHeartbeat()
            return maybeCommitEntry()
        }

        override fun handle(message: AppendEntries): State {
            return if (message.term > meta().currentTerm) {
                if (logger.isInfoEnabled) {
                    logger.info("leader ({}) got append entries from fresher leader ({}), will step down and the leader " + "will keep being: {}", meta().currentTerm, message.term, message.member)
                }
                gotoFollower().handle(message)
            } else {
                logger.warn("leader ({}) got append entries from rogue leader ({} @ {}), it's not fresher than self, " + "will send entries, to force it to step down.", meta().currentTerm, message.member,
                        message.term)
                sendEntries(message.member)
                this
            }
        }

        override fun handle(message: AppendRejected): State {
            if (message.term > meta().currentTerm) {
                return stay(meta().withTerm(message.term)).gotoFollower()
            }
            return if (message.term == meta().currentTerm) {
                val nextIndexFor = nextIndex.indexFor(message.member)
                if (nextIndexFor > message.lastIndex) {
                    nextIndex.put(message.member, message.lastIndex)
                } else if (nextIndexFor > 0) {
                    nextIndex.decrementFor(message.member)
                }
                logger.warn("follower {} rejected write, term {}, decrement index to {}", message.member,
                        message.term, nextIndex.indexFor(message.member))
                sendEntries(message.member)
                this
            } else {
                logger.warn("follower {} rejected write: {}, ignore", message.member, message.term)
                this
            }
        }

        override fun handle(message: AppendSuccessful): State {
            if (message.term > meta().currentTerm) {
                return stay(meta().withTerm(message.term)).gotoFollower()
            }
            if (message.term == meta().currentTerm) {
                if (logger.isInfoEnabled) {
                    logger.info("received append successful {} in term: {}", message, meta().currentTerm)
                }
                assert(message.lastIndex <= replicatedLog.lastIndex())
                if (message.lastIndex > 0) {
                    nextIndex.put(message.member, message.lastIndex + 1)
                }
                matchIndex.putIfGreater(message.member, message.lastIndex)
                replicationIndex = replicationIndex.put(message.member, 0L)
                maybeSendEntries(message.member)
                return maybeCommitEntry()
            } else {
                logger.warn("unexpected append successful: {} in term:{}", message, meta().currentTerm)
                return this
            }
        }

        override fun handle(message: InstallSnapshot): State {
            if (message.term > meta().currentTerm) {
                if (logger.isInfoEnabled) {
                    logger.info("leader ({}) got install snapshot from fresher leader ({}), " + "will step down and the leader will keep being: {}",
                            meta().currentTerm, message.term, message.leader)
                }
                return stay(meta().withTerm(message.term)).gotoFollower().handle(message)
            } else {
                if (logger.isInfoEnabled) {
                    logger.info("rejecting install snapshot {}, current term is {}",
                            message.term, meta().currentTerm)
                }
                logger.warn("leader ({}) got install snapshot from rogue leader ({} @ {}), " + "it's not fresher than self, will send entries, to force it to step down.",
                        meta().currentTerm, message.leader, message.term)
                sendEntries(message.leader)
                return this
            }
        }

        override fun handle(message: InstallSnapshotSuccessful): State {
            if (message.term > meta().currentTerm) {
                return stay(meta().withTerm(message.term)).gotoFollower()
            }
            return if (message.term == meta().currentTerm) {
                if (logger.isInfoEnabled) {
                    logger.info("received install snapshot successful[{}], last index[{}]", message.lastIndex,
                            replicatedLog.lastIndex())
                }
                assert(message.lastIndex <= replicatedLog.lastIndex())
                if (message.lastIndex > 0) {
                    nextIndex.put(message.member, message.lastIndex + 1)
                }
                matchIndex.putIfGreater(message.member, message.lastIndex)
                maybeCommitEntry()
            } else {
                logger.warn("unexpected install snapshot successful: {} in term:{}", message, meta().currentTerm)
                this
            }
        }

        override fun handle(message: InstallSnapshotRejected): State {
            when {
                message.term > meta().currentTerm -> // since there seems to be another leader!
                    return stay(meta().withTerm(message.term)).gotoFollower()
                message.term == meta().currentTerm -> {
                    if (logger.isInfoEnabled) {
                        logger.info("follower {} rejected write: {}, back out the first index in this term and retry",
                                message.member, message.term)
                    }
                    if (nextIndex.indexFor(message.member) > 1) {
                        nextIndex.decrementFor(message.member)
                    }
                    sendEntries(message.member)
                    return this
                }
                else -> {
                    logger.warn("unexpected install snapshot successful: {} in term:{}", message, meta().currentTerm)
                    return this
                }
            }
        }

        override fun handle(request: AddServer): State {
            var meta = meta()
            if (meta.config.isTransitioning) {
                logger.warn("try add server {} in transitioning state", request.member)
                send(request.member, AddServerResponse(
                        AddServerResponse.Status.TIMEOUT,
                        Option.some(clusterDiscovery.self)
                ))
                return stay(meta)
            } else {
                if (meta.members().contains(request.member)) {
                    send(request.member, AddServerResponse(
                            AddServerResponse.Status.OK,
                            Option.some(clusterDiscovery.self)
                    ))
                    return stay(meta)
                }
                val config = StableClusterConfiguration(meta.members().add(request.member))
                meta = meta.withConfig(meta.config.transitionTo(config))
                return stay(meta).handle(ClientMessage(config, 0))
            }
        }

        override fun handle(request: RemoveServer): State {
            var meta = meta()
            if (meta.config.isTransitioning) {
                logger.warn("try remove server {} in transitioning state", request.member)
                send(request.member, RemoveServerResponse(
                        RemoveServerResponse.Status.TIMEOUT,
                        Option.some(clusterDiscovery.self)
                ))
            } else {
                val config = StableClusterConfiguration(
                        meta.membersWithout(request.member)
                )
                meta.config.transitionTo(config)
                meta = meta.withConfig(meta.config.transitionTo(config))
                return stay(meta).handle(ClientMessage(config, 0))
            }
            return stay(meta)
        }

        override fun handle(message: RequestVote): State {
            return if (message.term > meta().currentTerm) {
                if (logger.isInfoEnabled) {
                    logger.info("received newer {}, current term is {}", message.term, meta().currentTerm)
                }
                stay(meta().withTerm(message.term)).gotoFollower().handle(message)
            } else {
                logger.warn("rejecting vote for {}, and {}, currentTerm: {}, received stale term number {}",
                        message.candidate, message.term,
                        meta().currentTerm, message.term)
                send(message.candidate, DeclineCandidate(clusterDiscovery.self, meta().currentTerm))
                this
            }
        }

        fun startHeartbeat() {
            if (logger.isInfoEnabled) {
                logger.info("starting heartbeat")
            }
            context.startTimer(RaftContext.SEND_HEARTBEAT, heartbeat, heartbeat) {
                lock.lock()
                try {
                    if (state === this) {
                        state = sendHeartbeat()
                    } else {
                        throw IllegalStateException()
                    }
                } catch (e: IllegalStateException) {
                    logger.error("error send heartbeat", e)
                } finally {
                    lock.unlock()
                }
            }
        }

        fun sendHeartbeat(): LeaderState {
            if (logger.isDebugEnabled) {
                logger.debug("send heartbeat: {}", meta().members())
            }
            val timeout = System.currentTimeMillis() - heartbeat
            meta().membersWithout(clusterDiscovery.self)
                    .filter {
                        // check heartbeat response timeout for prevent re-send heartbeat
                        replicationIndex.getOrElse(it, 0L) < timeout
                    }
                    .forEach { sendEntries(it) }
            return this
        }

        fun maybeSendEntries(follower: DiscoveryNode) {
            // check heartbeat response timeout for prevent re-send heartbeat
            val timeout = System.currentTimeMillis() - heartbeat
            if (replicationIndex.getOrElse(follower, 0L) < timeout) {
                // if member is already append prev entries,
                // their next index must be equal to last index in log
                if (nextIndex.indexFor(follower) <= replicatedLog.lastIndex()) {
                    sendEntries(follower)
                }
            }
        }

        fun sendEntries(follower: DiscoveryNode) {
            val meta = meta()
            replicationIndex = replicationIndex.put(follower, System.currentTimeMillis())
            val lastIndex = nextIndex.indexFor(follower)

            if (replicatedLog.hasSnapshot()) {
                val snapshot = replicatedLog.snapshot()
                if (snapshot.meta.lastIncludedIndex >= lastIndex) {
                    if (logger.isInfoEnabled) {
                        logger.info("send install snapshot to {} in term {}", follower, meta.currentTerm)
                    }
                    send(follower, InstallSnapshot(clusterDiscovery.self, meta.currentTerm, snapshot))
                    return
                }
            }

            if (lastIndex > replicatedLog.nextIndex()) {
                throw Error("Unexpected from index " + lastIndex + " > " + replicatedLog.nextIndex())
            } else {
                val entries = replicatedLog.entriesBatchFrom(lastIndex, maxEntries)
                val prevIndex = Math.max(0, lastIndex - 1)
                val prevTerm = replicatedLog.termAt(prevIndex)
                if (logger.isInfoEnabled) {
                    logger.info("send to {} append entries {} prev {}:{} in {} from index:{}", follower, entries.size(),
                            prevTerm, prevIndex, meta.currentTerm, lastIndex)
                }
                val append = AppendEntries(
                        clusterDiscovery.self,
                        meta.currentTerm,
                        prevTerm, prevIndex,
                        replicatedLog.committedIndex(),
                        entries
                )
                send(follower, append)
            }
        }

        fun maybeCommitEntry(): State {
            var meta = meta()
            while (true) {
                val indexOnMajority = matchIndex.consensusForIndex(meta.config)
                if (indexOnMajority > replicatedLog.committedIndex()) {

                    if (logger.isInfoEnabled) {
                        logger.info("index of majority: {}", indexOnMajority)
                    }
                    val entries = replicatedLog.slice(replicatedLog.committedIndex() + 1, indexOnMajority)
                    // 3.6.2 To eliminate problems like the one in Figure 3.7, Raft never commits log entries
                    // from previous terms by counting replicas. Only log entries from the leader’s current
                    // term are committed by counting replicas; once an entry from the current term has been
                    // committed in this way, then all prior entries are committed indirectly because of
                    // the Log Matching Property.
                    if (entries.get(entries.size() - 1).term != meta.currentTerm) {
                        logger.warn("do not commit prev term")
                        return stay(meta)
                    }
                    for ((_, index, session, config) in entries) {
                        if (logger.isInfoEnabled) {
                            logger.info("committing log at index: {}", index)
                        }
                        replicatedLog.commit(index)
                        if (config is StableClusterConfiguration) {
                            if (logger.isInfoEnabled) {
                                logger.info("apply new configuration, old: {}, new: {}", meta.config, config)
                            }
                            meta = meta.withConfig(config)
                            if (!meta.config.containsOnNewState(clusterDiscovery.self)) {
                                return stay(meta).gotoFollower()
                            }
                        } else if (config is Noop) {
                            if (logger.isTraceEnabled) {
                                logger.trace("ignore noop entry")
                            }
                        } else {
                            if (logger.isDebugEnabled) {
                                logger.info("applying command[index={}]: {}",
                                    index, config.javaClass.simpleName)
                            }
                            registry.apply(index, config).forEach { result ->
                                if (logger.isDebugEnabled) {
                                    logger.debug("success client command session {}", session)
                                }
                                val promise = sessionCommands.remove(session)
                                promise?.success(result)
                            }
                        }
                    }
                }else{
                    break
                }
            }
            return if (replicatedLog.committedEntries() >= snapshotInterval) {
                stay(meta).createSnapshot()
            } else {
                stay(meta)
            }
        }
    }
}
