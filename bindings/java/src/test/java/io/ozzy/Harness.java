package io.ozzy;

final class Harness {
    static native void holdWrites(boolean hold);

    static native int heldWrites();

    private Harness() {}
}
