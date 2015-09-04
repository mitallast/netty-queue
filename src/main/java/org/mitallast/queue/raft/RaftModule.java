package org.mitallast.queue.raft;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.mitallast.queue.raft.action.append.AppendAction;
import org.mitallast.queue.raft.action.command.CommandAction;
import org.mitallast.queue.raft.action.join.JoinAction;
import org.mitallast.queue.raft.action.keepalive.KeepAliveAction;
import org.mitallast.queue.raft.action.leave.LeaveAction;
import org.mitallast.queue.raft.action.query.QueryAction;
import org.mitallast.queue.raft.action.register.RegisterAction;
import org.mitallast.queue.raft.action.vote.VoteAction;
import org.mitallast.queue.raft.cluster.ClusterService;
import org.mitallast.queue.raft.log.RaftLog;
import org.mitallast.queue.raft.log.RocksDBRaftLog;
import org.mitallast.queue.raft.log.SegmentRaftLog;
import org.mitallast.queue.raft.log.compaction.*;
import org.mitallast.queue.raft.log.entry.EntryFilter;
import org.mitallast.queue.raft.resource.ResourceFactory;
import org.mitallast.queue.raft.resource.ResourceService;
import org.mitallast.queue.raft.resource.manager.ResourceStateMachine;
import org.mitallast.queue.raft.state.RaftState;
import org.mitallast.queue.raft.state.RaftStateClient;
import org.mitallast.queue.raft.state.RaftStateContext;
import org.mitallast.queue.raft.state.RaftStateFactory;
import org.mitallast.queue.raft.util.ExecutionContext;

public class RaftModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ExecutionContext.class).asEagerSingleton();
        bind(RaftStreamService.class).asEagerSingleton();

        // log
        // use rocksdb
        if (true) {
            bind(RocksDBRaftLog.class).asEagerSingleton();
            bind(RaftLog.class).to(RocksDBRaftLog.class);
        } else {
            bind(SegmentRaftLog.class).asEagerSingleton();
            bind(RaftLog.class).to(SegmentRaftLog.class);
        }
        bind(Compactor.class).asEagerSingleton();
        install(new FactoryModuleBuilder()
            .implement(MinorCompaction.class, MinorCompaction.class)
            .build(MinorCompactionFactory.class));
        install(new FactoryModuleBuilder()
            .implement(MajorCompaction.class, MajorCompaction.class)
            .build(MajorCompactionFactory.class));

        // state
        bind(RaftStateFactory.class).asEagerSingleton();
        bind(ClusterService.class).asEagerSingleton();
        bind(RaftState.class).asEagerSingleton();
        bind(RaftStateContext.class).asEagerSingleton();
        bind(RaftStateClient.class).to(RaftStateContext.class);

        // resource
        bind(ResourceFactory.class).asEagerSingleton();

        // state machine
        bind(ResourceStateMachine.class).asEagerSingleton();
        bind(StateMachine.class).to(ResourceStateMachine.class);
        bind(EntryFilter.class).to(RaftState.class);

        bind(Protocol.class).to(RaftStateContext.class);
        bind(ResourceService.class).asEagerSingleton();

        // action
        bind(AppendAction.class).asEagerSingleton();
        bind(CommandAction.class).asEagerSingleton();
        bind(JoinAction.class).asEagerSingleton();
        bind(KeepAliveAction.class).asEagerSingleton();
        bind(LeaveAction.class).asEagerSingleton();
        bind(QueryAction.class).asEagerSingleton();
        bind(RegisterAction.class).asEagerSingleton();
        bind(VoteAction.class).asEagerSingleton();
    }
}
