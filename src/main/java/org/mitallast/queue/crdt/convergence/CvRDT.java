package org.mitallast.queue.crdt.convergence;

/**
 * State-based CRDTs are called convergent replicated data types, or CvRDTs.
 * In contrast to CmRDTs, CvRDTs send their full local state to other replicas.
 * <p>
 * CvRDTs have the following local interface:
 * - query: reads the state of the replica, with no side effects.
 * - update: writes to the replica state in accordance with certain restrictions.
 * - merge: merges local state with the state of some remote replica.
 * <p>
 * The merge function must be commutative, associative, and idempotent.
 * It provides a join for any pair of replica states, so the set of all states forms a semilattice.
 * The update function must monotonically increase the internal state, according to the same partial
 * order rules as the semilattice.
 */
public interface CvRDT<T extends CvRDT<T>> {

    interface Query<T> {
    }

    interface QueryResponse<T> {
    }

    interface Update<T> {
    }

    void update(Update<T> update);

    QueryResponse<T> query(Query<T> query);

    boolean compare(T other);

    void merge(T other);
}
