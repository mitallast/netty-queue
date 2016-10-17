package org.mitallast.queue.raft.fsm;

import com.google.common.collect.ImmutableMap;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.TransportChannel;
import org.mitallast.queue.transport.netty.codec.MessageTransportFrame;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

public class FSM<StateType, MetadataType> extends AbstractComponent {
    private State state;
    private ImmutableMap<StateType, StateFunction> stateMap = ImmutableMap.of();

    public FSM(Settings settings) {
        super(settings);
    }

    public void startWith(StateType initialState, MetadataType initialMetadata) {
        state = new State(initialState, initialMetadata, new SelfSender());
    }

    public void onStart() {
    }

    public void when(StateType state, StateFunction function) {
        stateMap = ImmutableMap.<StateType, StateFunction>builder()
                .putAll(stateMap)
                .put(state, function)
                .build();
    }

    public void onTransition(StateType prevState, StateType newState) {
    }

    public void onTermination() {
    }

    public void initialize() {
    }

    public void cancelTimer(String timerName) {
    }

    public void setTimer(String timeName, Object event, long timeout, TimeUnit timeUnit) {
    }

    public void setTimer(String timeName, Object event, long timeout, TimeUnit timeUnit, boolean repeat) {
    }

    public void receive(TransportChannel channel, Object event) {
        receive(new TransportSender(channel), event);
    }

    public void receive(Object event) {
        receive(new SelfSender(), event);
    }

    public void receive(Sender sender, Object event) {
        state = state.withSender(sender);
        state = stateMap.get(state.currentState).receive(event, state.currentMetadata);
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
    }

    public abstract class StateFunction {

        abstract public State receive(Object event, MetadataType metadata);

        public StateFunction orElse(StateFunction next) {
            return new Combined(this, next);
        }
    }

    public MatchStateFunctionBuilder match() {
        return new MatchStateFunctionBuilder();
    }

    public class MatchStateFunctionBuilder {

        private ImmutableMap<Class, BiFunction<Object, MetadataType, State>> matchers = ImmutableMap.of();

        @SuppressWarnings("unchecked")
        public <Event> MatchStateFunctionBuilder event(Class<Event> eventClass, BiFunction<Event, MetadataType, State> consumer) {
            matchers = ImmutableMap.<Class, BiFunction<Object, MetadataType, State>>builder()
                    .putAll(matchers)
                    .put(eventClass, (BiFunction<Object, MetadataType, State>) consumer)
                    .build();
            return this;
        }

        public StateFunction build() {
            return new MatchStateFunction(matchers);
        }
    }

    private class MatchStateFunction extends StateFunction {

        private final ImmutableMap<Class, BiFunction<Object, MetadataType, State>> matchers;

        private MatchStateFunction(ImmutableMap<Class, BiFunction<Object, MetadataType, State>> matchers) {
            this.matchers = matchers;
        }

        @Override
        public State receive(Object event, MetadataType metadata) {
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
        public State receive(Object event, MetadataType metadata) {
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
