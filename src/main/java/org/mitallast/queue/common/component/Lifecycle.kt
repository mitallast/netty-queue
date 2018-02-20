package org.mitallast.queue.common.component


class Lifecycle {

    @Volatile private var state = State.INITIALIZED

    fun state(): State {
        return this.state
    }

    /**
     * Returns <tt>true</tt> if the state is initialized.
     */
    fun initialized(): Boolean {
        return state == State.INITIALIZED
    }

    /**
     * Returns <tt>true</tt> if the state is started.
     */
    fun started(): Boolean {
        return state == State.STARTED
    }

    /**
     * Returns <tt>true</tt> if the state is stopped.
     */
    fun stopped(): Boolean {
        return state == State.STOPPED
    }

    /**
     * Returns <tt>true</tt> if the state is closed.
     */
    fun closed(): Boolean {
        return state == State.CLOSED
    }

    fun stoppedOrClosed(): Boolean {
        val state = this.state
        return state == State.STOPPED || state == State.CLOSED
    }

    @Throws(IllegalStateException::class)
    fun canMoveToStarted(): Boolean {
        val localState = this.state
        if (localState == State.INITIALIZED || localState == State.STOPPED) {
            return true
        }
        if (localState == State.STARTED) {
            return false
        }
        if (localState == State.CLOSED) {
            throw IllegalStateException("Can't move to started state when closed")
        }
        throw IllegalStateException("Can't move to started with unknown state")
    }

    @Throws(IllegalStateException::class)
    fun moveToStarted(): Boolean {
        val localState = this.state
        if (localState == State.INITIALIZED || localState == State.STOPPED) {
            state = State.STARTED
            return true
        }
        if (localState == State.STARTED) {
            return false
        }
        if (localState == State.CLOSED) {
            throw IllegalStateException("Can't move to started state when closed")
        }
        throw IllegalStateException("Can't move to started with unknown state")
    }

    @Throws(IllegalStateException::class)
    fun canMoveToStopped(): Boolean {
        val localState = state
        if (localState == State.STARTED) {
            return true
        }
        if (localState == State.INITIALIZED || localState == State.STOPPED) {
            return false
        }
        if (localState == State.CLOSED) {
            throw IllegalStateException("Can't move to started state when closed")
        }
        throw IllegalStateException("Can't move to started with unknown state")
    }

    @Throws(IllegalStateException::class)
    fun moveToStopped(): Boolean {
        val localState = state
        if (localState == State.STARTED) {
            state = State.STOPPED
            return true
        }
        if (localState == State.INITIALIZED || localState == State.STOPPED) {
            return false
        }
        if (localState == State.CLOSED) {
            throw IllegalStateException("Can't move to started state when closed")
        }
        throw IllegalStateException("Can't move to started with unknown state")
    }

    @Throws(IllegalStateException::class)
    fun canMoveToClosed(): Boolean {
        val localState = state
        if (localState == State.CLOSED) {
            return false
        }
        if (localState == State.STARTED) {
            throw IllegalStateException("Can't move to closed before moving to stopped mode")
        }
        return true
    }

    @Throws(IllegalStateException::class)
    fun moveToClosed(): Boolean {
        val localState = state
        if (localState == State.CLOSED) {
            return false
        }
        if (localState == State.STARTED) {
            throw IllegalStateException("Can't move to closed before moving to stopped mode")
        }
        state = State.CLOSED
        return true
    }

    override fun toString(): String {
        return state.toString()
    }

    enum class State {
        INITIALIZED,
        STOPPED,
        STARTED,
        CLOSED
    }
}

