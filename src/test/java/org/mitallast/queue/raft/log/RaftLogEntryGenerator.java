package org.mitallast.queue.raft.log;

import com.google.common.net.HostAndPort;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.raft.log.entry.*;
import org.mitallast.queue.raft.resource.manager.CreatePath;
import org.mitallast.queue.raft.resource.manager.PathExists;
import org.mitallast.queue.transport.DiscoveryNode;

import java.util.Random;
import java.util.UUID;

public class RaftLogEntryGenerator {
    private final Random random;

    public RaftLogEntryGenerator(Random random) {
        this.random = random;
    }

    public RaftLogEntry[] generate(int max) {
        RaftLogEntry[] entries = new RaftLogEntry[max];
        for (int i = 0; i < max; i++) {

            switch (random.nextInt(7)) {
                case 0:
                    entries[i] = CommandEntry.builder()
                        .setIndex(i + 1)
                        .setTerm(i)
                        .setTimestamp(random.nextLong())
                        .setSession(random.nextLong())
                        .setRequest(random.nextLong())
                        .setResponse(random.nextLong())
                        .setCommand(new CreatePath("some path"))
                        .build();
                    break;
                case 1:
                    entries[i] = JoinEntry.builder()
                        .setIndex(i + 1)
                        .setTerm(i)
                        .setMember(randomNode())
                        .build();
                    break;
                case 2:
                    entries[i] = KeepAliveEntry.builder()
                        .setIndex(i + 1)
                        .setTerm(i)
                        .setTimestamp(random.nextLong())
                        .setSession(random.nextLong())
                        .build();
                    break;
                case 3:
                    entries[i] = LeaveEntry.builder()
                        .setIndex(i + 1)
                        .setTerm(i)
                        .setMember(randomNode())
                        .build();
                    break;
                case 4:
                    entries[i] = NoOpEntry.builder()
                        .setIndex(i + 1)
                        .setTerm(i)
                        .build();
                    break;
                case 5:
                    entries[i] = QueryEntry.builder()
                        .setIndex(i + 1)
                        .setTerm(i)
                        .setTimestamp(random.nextLong())
                        .setVersion(random.nextInt())
                        .setSession(random.nextInt())
                        .setQuery(new PathExists("some path"))
                        .build();
                    break;
                case 6:
                    entries[i] = RegisterEntry.builder()
                        .setIndex(i + 1)
                        .setTerm(i)
                        .setTimestamp(random.nextLong())
                        .build();
                    break;
                default:
                    assert false;
            }
        }
        return entries;
    }

    public DiscoveryNode randomNode() {
        return new DiscoveryNode(randomUUID().toString(), HostAndPort.fromParts("localhost", random.nextInt(10000)), Version.CURRENT);
    }

    public UUID randomUUID() {
        return UUIDs.generateRandom();
    }
}
