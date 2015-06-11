package org.mitallast.queue.common.component;

import com.google.inject.Binding;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.AbstractMatcher;

public class LifecycleMatcher extends AbstractMatcher<Binding<?>> {
    @Override
    public boolean matches(Binding<?> binding) {
        final Key<?> key = binding.getKey();
        final TypeLiteral<?> typeLiteral = key.getTypeLiteral();
        return LifecycleComponent.class.isAssignableFrom(typeLiteral.getRawType());
    }
}
