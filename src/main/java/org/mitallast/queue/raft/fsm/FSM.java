package org.mitallast.queue.raft.fsm;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.concurrent.NamedExecutors;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.TransportChannel;
import org.mitallast.queue.transport.netty.codec.MessageTransportFrame;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.jar.Attributes;

public abstract class FSM<StateType, MetadataType> extends AbstractLifecycleComponent {

    private final ScheduledExecutorService context;
    private final ConcurrentMap<String, ScheduledFuture> timerMap = new ConcurrentHashMap<>();

    private State state;
    private ImmutableMap<StateType, StateFunction> stateMap = ImmutableMap.of();

    public FSM(Config config, Class loggerClass) {
        super(config, loggerClass);
        context = Executors.newScheduledThreadPool(1, NamedExecutors.newThreadFactory("raft"));
    }

    @Override
    protected void doStart() throws IOException {
        onStart();
    }

    @Override
    protected void doStop() throws IOException {
        onTermination();
    }

    @Override
    protected void doClose() throws IOException {
        try {
            context.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            context.shutdownNow();
        }
    }

    public void startWith(StateType initialState, MetadataType initialMetadata) {
        state = new State(initialState, initialMetadata, new SelfSender());
    }

    public abstract void onStart();

    public void when(StateType state, StateFunction function) {
        stateMap = ImmutableMap.<StateType, StateFunction>builder()
                .putAll(stateMap)
                .put(state, function)
                .build();
    }

    public abstract void onTransition(StateType prevState, StateType newState);

    public abstract void onTermination();

    public void cancelTimer(String timerName) {
        ScheduledFuture timer = timerMap.remove(timerName);
        if (timer != null) {
            timer.cancel(false);
        }
    }

    public void setTimer(String timerName, Streamable event, long timeout, TimeUnit timeUnit) {
        setTimer(timerName, event, timeout, timeUnit, false);
    }

    public void setTimer(String timerName, Streamable event, long timeout, TimeUnit timeUnit, boolean repeat) {
        cancelTimer(timerName);
        final ScheduledFuture timer;
        if (repeat) {
            timer = context.scheduleAtFixedRate(() -> receive(event), timeout, timeout, timeUnit);
        } else {
            timer = context.schedule(() -> receive(event), timeout, timeUnit);
        }
        timerMap.put(timerName, timer);
    }

    public void receive(TransportChannel channel, Streamable event) {
        receive(new TransportSender(channel), event);
    }

    public void receive(Streamable event) {
        receive(new SelfSender(), event);
    }

    public void receive(Sender sender, Streamable event) {
        context.execute(() -> {
            try {
                State prevState = state;
                state = state.withSender(sender);
                State newState = stateMap.get(state.currentState).receive(event, state.currentMetadata);
                if (newState == null) {
                    logger.warn("unhandled event: {}", event);
                } else {
                    state = newState;
                    if (!state.currentState.equals(prevState.currentState)) {
                        onTransition(prevState.currentState, state.currentState);
                    }
                }
            } catch (Exception e) {
                logger.error("unexpected error in fsm", e);
            }
        });
    }

    public Sender sender() {
        return state.sender;
    }

    public StateType currentState() {
        return state.currentState;
    }

    public State stay() {
        return state.stay();
    }

    public State stay(MetadataType newMeta) {
        return state.stay(newMeta);
    }

    public State goTo(StateType newState) {
        return state.goTo(newState);
    }

    public State goTo(StateType newState, MetadataType newMeta) {
        return state.goTo(newState, newMeta);
    }

    public class State {
        private final StateType currentState;
        private final MetadataType currentMetadata;
        private final Sender sender;

        private State(StateType currentState, MetadataType currentMetadata, Sender sender) {
            this.currentState = currentState;
            this.currentMetadata = currentMetadata;
            this.sender = sender;
        }

        public Sender sender() {
            return sender;
        }

        public State withSender(Sender sender) {
            return new State(currentState, currentMetadata, sender);
        }

        public State stay() {
            return this;
        }

        public State stay(MetadataType newMetadata) {
            return new State(currentState, newMetadata, sender);
        }

        public State goTo(StateType newState) {
            return new State(newState, currentMetadata, sender);
        }

        public State goTo(StateType newState, MetadataType newMetadata) {
            return new State(newState, newMetadata, sender);
        }

        @Override
        public String toString() {
            return "State{" +
                    "currentState=" + currentState +
                    ", currentMetadata=" + currentMetadata +
                    ", sender=" + sender +
                    '}';
        }
    }

    public abstract class StateFunction {

        abstract public State receive(Streamable event, MetadataType metadata);

        public StateFunction orElse(StateFunction next) {
            return new Combined(this, next);
        }
    }

    public MatchStateFunctionBuilder match() {
        return new MatchStateFunctionBuilder();
    }

    public class MatchStateFunctionBuilder {

        private ImmutableMap<Class, BiFunction<Streamable, MetadataType, State>> matchers = ImmutableMap.of();

        @SuppressWarnings("unchecked")
        public <Event> MatchStateFunctionBuilder event(Class<Event> eventClass, BiFunction<Event, MetadataType, State> consumer) {
            matchers = ImmutableMap.<Class, BiFunction<Streamable, MetadataType, State>>builder()
                    .putAll(matchers)
                    .put(eventClass, (BiFunction<Streamable, MetadataType, State>) consumer)
                    .build();
            return this;
        }

        public StateFunction build() {
            return new MatchStateFunction(matchers);
        }
    }

    private class MatchStateFunction extends StateFunction {

        private final ImmutableMap<Class, BiFunction<Streamable, MetadataType, State>> matchers;

        private MatchStateFunction(ImmutableMap<Class, BiFunction<Streamable, MetadataType, State>> matchers) {
            this.matchers = matchers;
        }

        @Override
        public State receive(Streamable event, MetadataType metadata) {
            if (matchers.containsKey(event.getClass())) {
                return matchers.get(event.getClass()).apply(event, metadata);
            }
            return null;
        }

    }

    private class Combined extends StateFunction {
        private final StateFunction first;
        private final StateFunction second;

        private Combined(StateFunction first, StateFunction second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public State receive(Streamable event, MetadataType metadata) {
            State state = first.receive(event, metadata);
            if (state == null) {
                state = second.receive(event, metadata);
            }
            return state;
        }
    }

    public interface Sender {
        void send(Streamable entry);
    }

    private class SelfSender implements Sender {

        @Override
        public void send(Streamable entry) {
            receive(this, entry);
        }
    }

    private class TransportSender implements Sender {
        private final TransportChannel channel;

        private TransportSender(TransportChannel channel) {
            this.channel = channel;
        }

        @Override
        public void send(Streamable entry) {
            channel.send(new MessageTransportFrame(Version.CURRENT, entry));
        }
    }
}
