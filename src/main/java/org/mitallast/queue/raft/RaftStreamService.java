package org.mitallast.queue.raft;

import com.google.inject.Inject;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.raft.action.append.AppendRequest;
import org.mitallast.queue.raft.action.append.AppendResponse;
import org.mitallast.queue.raft.action.command.CommandRequest;
import org.mitallast.queue.raft.action.command.CommandResponse;
import org.mitallast.queue.raft.action.join.JoinRequest;
import org.mitallast.queue.raft.action.join.JoinResponse;
import org.mitallast.queue.raft.action.keepalive.KeepAliveRequest;
import org.mitallast.queue.raft.action.keepalive.KeepAliveResponse;
import org.mitallast.queue.raft.action.leave.LeaveRequest;
import org.mitallast.queue.raft.action.leave.LeaveResponse;
import org.mitallast.queue.raft.action.query.QueryRequest;
import org.mitallast.queue.raft.action.query.QueryResponse;
import org.mitallast.queue.raft.action.register.RegisterRequest;
import org.mitallast.queue.raft.action.register.RegisterResponse;
import org.mitallast.queue.raft.action.vote.VoteRequest;
import org.mitallast.queue.raft.action.vote.VoteResponse;
import org.mitallast.queue.raft.log.entry.*;
import org.mitallast.queue.raft.resource.ResourceCommand;
import org.mitallast.queue.raft.resource.ResourceQuery;
import org.mitallast.queue.raft.resource.manager.*;
import org.mitallast.queue.raft.resource.result.BooleanResult;
import org.mitallast.queue.raft.resource.result.LongResult;
import org.mitallast.queue.raft.resource.result.StringListResult;
import org.mitallast.queue.raft.resource.result.VoidResult;
import org.mitallast.queue.raft.resource.structures.AsyncBoolean;
import org.mitallast.queue.raft.resource.structures.AsyncMap;
import org.mitallast.queue.raft.util.StringValue;

public class RaftStreamService {
    @Inject
    public RaftStreamService(StreamService streamService) {
        registerActionEntry(streamService);
        registerLogEntry(streamService);
        registerCommandEntry(streamService);
        registerResult(streamService);
        registerAsyncBoolean(streamService);
        registerAsyncMap(streamService);
        registerResource(streamService);
    }

    private void registerActionEntry(StreamService streamService) {
        int index = 2000;
        streamService.registerClass(AppendRequest.Builder.class, AppendRequest.Builder::new, ++index);
        streamService.registerClass(AppendResponse.Builder.class, AppendResponse.Builder::new, ++index);
        streamService.registerClass(CommandRequest.Builder.class, CommandRequest.Builder::new, ++index);
        streamService.registerClass(CommandResponse.Builder.class, CommandResponse.Builder::new, ++index);
        streamService.registerClass(JoinRequest.Builder.class, JoinRequest.Builder::new, ++index);
        streamService.registerClass(JoinResponse.Builder.class, JoinResponse.Builder::new, ++index);
        streamService.registerClass(KeepAliveRequest.Builder.class, KeepAliveRequest.Builder::new, ++index);
        streamService.registerClass(KeepAliveResponse.Builder.class, KeepAliveResponse.Builder::new, ++index);
        streamService.registerClass(LeaveRequest.Builder.class, LeaveRequest.Builder::new, ++index);
        streamService.registerClass(LeaveResponse.Builder.class, LeaveResponse.Builder::new, ++index);
        streamService.registerClass(QueryRequest.Builder.class, QueryRequest.Builder::new, ++index);
        streamService.registerClass(QueryResponse.Builder.class, QueryResponse.Builder::new, ++index);
        streamService.registerClass(RegisterRequest.Builder.class, RegisterRequest.Builder::new, ++index);
        streamService.registerClass(RegisterResponse.Builder.class, RegisterResponse.Builder::new, ++index);
        streamService.registerClass(VoteRequest.Builder.class, VoteRequest.Builder::new, ++index);
        streamService.registerClass(VoteResponse.Builder.class, VoteResponse.Builder::new, ++index);
    }

    private void registerLogEntry(StreamService streamService) {
        int index = 3000;
        streamService.registerClass(CommandEntry.Builder.class, CommandEntry.Builder::new, ++index);
        streamService.registerClass(JoinEntry.Builder.class, JoinEntry.Builder::new, ++index);
        streamService.registerClass(KeepAliveEntry.Builder.class, KeepAliveEntry.Builder::new, ++index);
        streamService.registerClass(LeaveEntry.Builder.class, LeaveEntry.Builder::new, ++index);
        streamService.registerClass(NoOpEntry.Builder.class, NoOpEntry.Builder::new, ++index);
        streamService.registerClass(QueryEntry.Builder.class, QueryEntry.Builder::new, ++index);
        streamService.registerClass(RegisterEntry.Builder.class, RegisterEntry.Builder::new, ++index);
    }

    private void registerCommandEntry(StreamService streamService) {
        int index = 4000;
        streamService.registerClass(AddListener.Builder.class, AddListener.Builder::new, ++index);
        streamService.registerClass(CreatePath.Builder.class, CreatePath.Builder::new, ++index);
        streamService.registerClass(CreateResource.Builder.class, CreateResource.Builder::new, ++index);
        streamService.registerClass(DeletePath.Builder.class, DeletePath.Builder::new, ++index);
        streamService.registerClass(DeleteResource.Builder.class, DeleteResource.Builder::new, ++index);
        streamService.registerClass(PathChildren.Builder.class, PathChildren.Builder::new, ++index);
        streamService.registerClass(PathExists.Builder.class, PathExists.Builder::new, ++index);
        streamService.registerClass(RemoveListener.Builder.class, RemoveListener.Builder::new, ++index);
        streamService.registerClass(ResourceExists.Builder.class, ResourceExists.Builder::new, ++index);
    }

    private void registerResult(StreamService streamService) {
        int index = 5000;
        streamService.registerClass(BooleanResult.class, BooleanResult::new, ++index);
        streamService.registerClass(LongResult.class, LongResult::new, ++index);
        streamService.registerClass(StringListResult.class, StringListResult::new, ++index);
        streamService.registerClass(VoidResult.class, VoidResult::new, ++index);
    }

    private void registerAsyncBoolean(StreamService streamService) {
        int index = 6000;
        streamService.registerClass(AsyncBoolean.Get.Builder.class, AsyncBoolean.Get.Builder::new, ++index);
        streamService.registerClass(AsyncBoolean.Set.Builder.class, AsyncBoolean.Set.Builder::new, ++index);
        streamService.registerClass(AsyncBoolean.GetAndSet.Builder.class, AsyncBoolean.GetAndSet.Builder::new, ++index);
        streamService.registerClass(AsyncBoolean.CompareAndSet.Builder.class, AsyncBoolean.CompareAndSet.Builder::new, ++index);
    }

    private void registerAsyncMap(StreamService streamService) {
        int index = 7000;
        streamService.registerClass(AsyncMap.ContainsKey.Builder.class, AsyncMap.ContainsKey.Builder::new, ++index);
        streamService.registerClass(AsyncMap.Put.Builder.class, AsyncMap.Put.Builder::new, ++index);
        streamService.registerClass(AsyncMap.PutIfAbsent.Builder.class, AsyncMap.PutIfAbsent.Builder::new, ++index);
        streamService.registerClass(AsyncMap.Get.Builder.class, AsyncMap.Get.Builder::new, ++index);
        streamService.registerClass(AsyncMap.GetOrDefault.Builder.class, AsyncMap.GetOrDefault.Builder::new, ++index);
        streamService.registerClass(AsyncMap.Remove.Builder.class, AsyncMap.Remove.Builder::new, ++index);
        streamService.registerClass(AsyncMap.IsEmpty.Builder.class, AsyncMap.IsEmpty.Builder::new, ++index);
        streamService.registerClass(AsyncMap.Size.Builder.class, AsyncMap.Size.Builder::new, ++index);
        streamService.registerClass(AsyncMap.Clear.Builder.class, AsyncMap.Clear.Builder::new, ++index);

        streamService.registerClass(StringValue.class, StringValue::new, ++index);
    }

    private void registerResource(StreamService streamService) {
        int index = 8000;
        streamService.registerClass(ResourceCommand.Builder.class, () -> new ResourceCommand.Builder(), ++index);
        streamService.registerClass(ResourceQuery.Builder.class, () -> new ResourceQuery.Builder(), ++index);
    }
}
