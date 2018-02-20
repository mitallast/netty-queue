package org.mitallast.queue.common.component

import com.google.inject.Binding
import com.google.inject.matcher.AbstractMatcher

class LifecycleMatcher : AbstractMatcher<Binding<*>>() {
    override fun matches(binding: Binding<*>): Boolean {
        val key = binding.key
        val typeLiteral = key.typeLiteral
        return LifecycleComponent::class.java.isAssignableFrom(typeLiteral.rawType)
    }
}
