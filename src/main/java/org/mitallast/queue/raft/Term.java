package org.mitallast.queue.raft;

public class Term {

    private final long term;

    public Term(long term) {
        this.term = term;
    }

    public long getTerm() {
        return term;
    }

    public Term next() {
        return new Term(term + 1);
    }

    public boolean greater(Term other) {
        return term > other.term;
    }

    public boolean less(Term other) {
        return term < other.term;
    }

    public boolean greaterOrEqual(Term other) {
        return term >= other.term;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Term term1 = (Term) o;

        return term == term1.term;

    }

    @Override
    public int hashCode() {
        return (int) (term ^ (term >>> 32));
    }

    @Override
    public String toString() {
        return "term(" + term + ")";
    }
}
