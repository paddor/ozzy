package io.ozzy;

/** An operation requires an open SDK handle. */
public final class ClosedException extends OzzyException {
    /** Construct a closed-handle error. */
    public ClosedException() {
        super("handle is closed");
    }

    private static final long serialVersionUID = 1L;
}
