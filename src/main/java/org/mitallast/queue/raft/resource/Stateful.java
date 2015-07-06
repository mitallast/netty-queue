package org.mitallast.queue.raft.resource;

import org.mitallast.queue.raft.StateMachine;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Stateful {

    Class<? extends StateMachine> value();
}
