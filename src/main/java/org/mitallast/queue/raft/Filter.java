package org.mitallast.queue.raft;

import org.mitallast.queue.raft.log.Compaction;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Filter {

    Class<? extends Command>[] value() default {};

    Compaction.Type compaction() default Compaction.Type.MAJOR;

    class All implements Command {
    }
}
