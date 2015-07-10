package org.mitallast.queue.raft.resource.manager;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.Apply;
import org.mitallast.queue.raft.Commit;
import org.mitallast.queue.raft.Filter;
import org.mitallast.queue.raft.StateMachine;
import org.mitallast.queue.raft.log.compaction.Compaction;
import org.mitallast.queue.raft.resource.ResourceCommand;
import org.mitallast.queue.raft.resource.ResourceOperation;
import org.mitallast.queue.raft.resource.ResourceQuery;
import org.mitallast.queue.raft.resource.result.BooleanResult;
import org.mitallast.queue.raft.resource.result.LongResult;
import org.mitallast.queue.raft.resource.result.StringListResult;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ResourceStateMachine extends StateMachine {
    private static final String PATH_SEPARATOR = "/";
    private final Map<Long, NodeHolder> nodes = new HashMap<>();
    private final Map<Long, StateMachine> resources = new HashMap<>();
    private NodeHolder node;

    @Inject
    public ResourceStateMachine(Settings settings) {
        super(settings);
    }

    private void init(Commit commit) {
        if (node == null) {
            node = new NodeHolder(PATH_SEPARATOR, commit.index());
        }
    }

    @SuppressWarnings("unchecked")
    @Apply({ResourceCommand.class, ResourceQuery.class})
    protected Streamable commandResource(Commit<? extends ResourceOperation> commit) {
        StateMachine resource = resources.get(commit.operation().resource());
        if (resource != null) {
            return resource.apply(new Commit(commit.index(), commit.session(), commit.timestamp(), commit.operation().operation()));
        }
        throw new IllegalArgumentException("unknown resource: " + commit.operation().resource());
    }

    @SuppressWarnings("unchecked")
    @Filter(ResourceCommand.class)
    protected boolean filterResource(Commit<ResourceCommand> commit, Compaction compaction) {
        StateMachine resource = resources.get(commit.operation().resource());
        return resource != null && resource.filter(new Commit(commit.index(), commit.session(), commit.timestamp(), commit.operation().operation()), compaction);
    }

    @Apply(CreatePath.class)
    protected BooleanResult createPath(Commit<CreatePath> commit) {
        String path = commit.operation().path();

        init(commit);

        NodeHolder node = this.node;

        boolean created = false;
        for (String name : path.split(PATH_SEPARATOR)) {
            if (!name.isEmpty()) {
                NodeHolder child = node.children.get(name);
                if (child == null) {
                    child = new NodeHolder(name, commit.index());
                    node.children.put(child.name, child);
                    created = true;
                }
                node = child;
            }
        }

        return new BooleanResult(created);
    }

    @Filter(CreatePath.class)
    protected boolean filterCreatePath(Commit<CreatePath> commit, Compaction compaction) {
        String path = commit.operation().path();

        init(commit);

        NodeHolder node = this.node;
        for (String name : path.split(PATH_SEPARATOR)) {
            if (!name.isEmpty()) {
                NodeHolder child = node.children.get(name);
                if (child == null) {
                    return false;
                }
                node = child;
            }
        }
        return node.version == commit.index();
    }

    @Apply(PathExists.class)
    protected BooleanResult pathExists(Commit<PathExists> commit) {
        String path = commit.operation().path();

        if (this.node == null)
            return new BooleanResult(false);

        NodeHolder node = this.node;
        for (String name : path.split(PATH_SEPARATOR)) {
            if (!name.isEmpty()) {
                node = node.children.get(name);
                if (node == null) {
                    return new BooleanResult(false);
                }
            }
        }
        return new BooleanResult(true);
    }

    @SuppressWarnings("unchecked")
    @Apply(PathChildren.class)
    protected StringListResult pathChildren(Commit<PathChildren> commit) {
        String path = commit.operation().path();

        if (this.node == null)
            return new StringListResult();

        NodeHolder node = this.node;
        for (String name : path.split(PATH_SEPARATOR)) {
            if (!name.isEmpty()) {
                node = node.children.get(name);
                if (node == null) {
                    return new StringListResult();
                }
            }
        }

        return new StringListResult(ImmutableList.copyOf(node.children.keySet()));
    }

    @Apply(DeletePath.class)
    protected BooleanResult deletePath(Commit<DeletePath> commit) {
        String path = commit.operation().path();

        init(commit);

        NodeHolder parent = null;
        NodeHolder node = this.node;
        for (String name : path.split(PATH_SEPARATOR)) {
            if (!name.isEmpty()) {
                parent = node;
                node = node.children.get(name);
                if (node == null) {
                    return new BooleanResult(false);
                }
            }
        }

        if (parent != null) {
            parent.children.remove(node.name);
            return new BooleanResult(true);
        }
        return new BooleanResult(false);
    }

    @Filter(value = DeletePath.class, compaction = Compaction.Type.MAJOR)
    protected boolean filterDeletePath(Commit<DeletePath> commit, Compaction compaction) {
        return commit.index() >= compaction.index();
    }

    @Apply(CreateResource.class)
    protected LongResult createResource(Commit<CreateResource> commit) {
        String path = commit.operation().path();

        init(commit);

        NodeHolder node = this.node;

        for (String name : path.split(PATH_SEPARATOR)) {
            if (!name.isEmpty()) {
                NodeHolder child = node.children.get(name);
                if (child == null) {
                    child = new NodeHolder(name, commit.index());
                    node.children.put(child.name, child);
                }
                node = child;
            }
        }

        if (node.resource == 0) {
            node.resource = commit.index();
            try {
                StateMachine resource = commit.operation().type()
                    .getConstructor(Settings.class)
                    .newInstance(settings);
                resource.start();
                nodes.put(node.resource, node);
                resources.put(node.resource, resource);
            } catch (Exception e) {
                logger.error("failed to instantiate state machine", e);
                throw new ResourceManagerException("failed to instantiate state machine", e);
            }
        }

        return new LongResult(node.resource);
    }

    @Filter(CreateResource.class)
    protected boolean filterCreateResource(Commit<CreateResource> commit, Compaction compaction) {
        return resources.containsKey(commit.index());
    }

    @Apply(DeleteResource.class)
    protected BooleanResult deleteResource(Commit<DeleteResource> commit) {
        init(commit);

        NodeHolder node = nodes.remove(commit.operation().resource());
        if (node != null) {
            node.resource = 0;
        }

        StateMachine removedResource = resources.remove(commit.operation().resource());
        if (removedResource != null) {
            removedResource.stop();
            removedResource.close();
        }
        return new BooleanResult(removedResource != null);
    }

    @Filter(value = DeleteResource.class, compaction = Compaction.Type.MAJOR)
    protected boolean filterDeleteResource(Commit<DeleteResource> commit, Compaction compaction) {
        return commit.index() >= compaction.index();
    }

    private static class NodeHolder {
        private final String name;
        private final long version;
        private final Map<String, NodeHolder> children = new LinkedHashMap<>();
        private long resource;

        public NodeHolder(String name, long version) {
            this.name = name;
            this.version = version;
        }
    }

}
