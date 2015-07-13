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
import org.mitallast.queue.raft.resource.structures.LogResource;
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
        registerExceptions(streamService);
        registerLogResource(streamService);
    }

    private void registerActionEntry(StreamService streamService) {
        int index = 2000;
        streamService.registerClass(AppendRequest.Builder.class, AppendRequest::builder, ++index);
        streamService.registerClass(AppendResponse.Builder.class, AppendResponse::builder, ++index);
        streamService.registerClass(CommandRequest.Builder.class, CommandRequest::builder, ++index);
        streamService.registerClass(CommandResponse.Builder.class, CommandResponse::builder, ++index);
        streamService.registerClass(JoinRequest.Builder.class, JoinRequest::builder, ++index);
        streamService.registerClass(JoinResponse.Builder.class, JoinResponse::builder, ++index);
        streamService.registerClass(KeepAliveRequest.Builder.class, KeepAliveRequest::builder, ++index);
        streamService.registerClass(KeepAliveResponse.Builder.class, KeepAliveResponse::builder, ++index);
        streamService.registerClass(LeaveRequest.Builder.class, LeaveRequest::builder, ++index);
        streamService.registerClass(LeaveResponse.Builder.class, LeaveResponse::builder, ++index);
        streamService.registerClass(QueryRequest.Builder.class, QueryRequest::builder, ++index);
        streamService.registerClass(QueryResponse.Builder.class, QueryResponse::builder, ++index);
        streamService.registerClass(RegisterRequest.Builder.class, RegisterRequest::builder, ++index);
        streamService.registerClass(RegisterResponse.Builder.class, RegisterResponse::builder, ++index);
        streamService.registerClass(VoteRequest.Builder.class, VoteRequest::builder, ++index);
        streamService.registerClass(VoteResponse.Builder.class, VoteResponse::builder, ++index);
    }

    private void registerLogEntry(StreamService streamService) {
        int index = 3000;
        streamService.registerClass(CommandEntry.Builder.class, CommandEntry::builder, ++index);
        streamService.registerClass(JoinEntry.Builder.class, JoinEntry::builder, ++index);
        streamService.registerClass(KeepAliveEntry.Builder.class, KeepAliveEntry::builder, ++index);
        streamService.registerClass(LeaveEntry.Builder.class, LeaveEntry::builder, ++index);
        streamService.registerClass(NoOpEntry.Builder.class, NoOpEntry::builder, ++index);
        streamService.registerClass(QueryEntry.Builder.class, QueryEntry::builder, ++index);
        streamService.registerClass(RegisterEntry.Builder.class, RegisterEntry::builder, ++index);
    }

    private void registerCommandEntry(StreamService streamService) {
        int index = 4000;
        streamService.registerClass(CreatePath.Builder.class, CreatePath::builder, ++index);
        streamService.registerClass(CreateResource.Builder.class, CreateResource::builder, ++index);
        streamService.registerClass(DeletePath.Builder.class, DeletePath::builder, ++index);
        streamService.registerClass(DeleteResource.Builder.class, DeleteResource::builder, ++index);
        streamService.registerClass(PathChildren.Builder.class, PathChildren::builder, ++index);
        streamService.registerClass(PathExists.Builder.class, PathExists::builder, ++index);
        streamService.registerClass(ResourceExists.Builder.class, ResourceExists::builder, ++index);
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
        streamService.registerClass(AsyncBoolean.Get.Builder.class, AsyncBoolean.Get::builder, ++index);
        streamService.registerClass(AsyncBoolean.Set.Builder.class, AsyncBoolean.Set::builder, ++index);
        streamService.registerClass(AsyncBoolean.GetAndSet.Builder.class, AsyncBoolean.GetAndSet::builder, ++index);
        streamService.registerClass(AsyncBoolean.CompareAndSet.Builder.class, AsyncBoolean.CompareAndSet::builder, ++index);
    }

    private void registerAsyncMap(StreamService streamService) {
        int index = 7000;
        streamService.registerClass(AsyncMap.ContainsKey.Builder.class, AsyncMap.ContainsKey::builder, ++index);
        streamService.registerClass(AsyncMap.Put.Builder.class, AsyncMap.Put::builder, ++index);
        streamService.registerClass(AsyncMap.PutIfAbsent.Builder.class, AsyncMap.PutIfAbsent::builder, ++index);
        streamService.registerClass(AsyncMap.Get.Builder.class, AsyncMap.Get::builder, ++index);
        streamService.registerClass(AsyncMap.GetOrDefault.Builder.class, AsyncMap.GetOrDefault::builder, ++index);
        streamService.registerClass(AsyncMap.Remove.Builder.class, AsyncMap.Remove::builder, ++index);
        streamService.registerClass(AsyncMap.IsEmpty.Builder.class, AsyncMap.IsEmpty::builder, ++index);
        streamService.registerClass(AsyncMap.Size.Builder.class, AsyncMap.Size::builder, ++index);
        streamService.registerClass(AsyncMap.Clear.Builder.class, AsyncMap.Clear::builder, ++index);

        streamService.registerClass(StringValue.class, StringValue::new, ++index);
    }

    private void registerResource(StreamService streamService) {
        int index = 8000;
        streamService.registerClass(ResourceQuery.Builder.class, ResourceQuery::builder, ++index);
        streamService.registerClass(ResourceCommand.Builder.class, ResourceCommand::builder, ++index);
    }

    private void registerExceptions(StreamService streamService) {
        int index = 9000;
        streamService.registerClass(ApplicationException.Builder.class, ApplicationException::builder, ++index);
        streamService.registerClass(IllegalMemberStateException.Builder.class, IllegalMemberStateException::builder, ++index);
        streamService.registerClass(InternalException.Builder.class, InternalException::builder, ++index);
        streamService.registerClass(NoLeaderException.Builder.class, NoLeaderException::builder, ++index);
        streamService.registerClass(ProtocolException.Builder.class, ProtocolException::builder, ++index);
        streamService.registerClass(ReadException.Builder.class, ReadException::builder, ++index);
        streamService.registerClass(UnknownSessionException.Builder.class, UnknownSessionException::builder, ++index);
        streamService.registerClass(WriteException.Builder.class, WriteException::builder, ++index);
    }

    private void registerLogResource(StreamService streamService) {
        int index = 10000;
        streamService.registerClass(LogResource.AppendEntry.Builder.class, LogResource.AppendEntry::builder, ++index);
    }
}
