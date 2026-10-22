package io.ozzy;

import java.lang.ref.Cleaner;

// Immutable observers share cleanup, not progress or a mutable SDK lease. Each producer
// appends under its existing exclusive lease. A retained observer pins at most 64 handles,
// each one shared native observer count. One native call releases a whole group.
final class PendingHandles {
    private static final Cleaner CLEANER = Cleaner.create();
    private final State state = new State();

    PendingHandles() {
        CLEANER.register(this, state);
    }

    boolean full() {
        return state.size == state.pointers.length;
    }

    void add(long pointer) {
        int index = state.size;
        state.pointers[index] = pointer;
        // Publish every stored pointer to the cleanup thread before transfer returns.
        state.size = index + 1;
    }

    private static final class State implements Runnable {
        final long[] pointers = new long[64];
        volatile int size;

        @Override
        public void run() {
            Fast.releasePending(pointers, size);
        }
    }
}
