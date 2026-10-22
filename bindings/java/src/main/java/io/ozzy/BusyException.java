package io.ozzy;

/** Another operation exclusively leases this mutable SDK handle. */
public final class BusyException extends OzzyException {
    /** Construct a busy-handle error. */
    public BusyException() {
        super("operation already in progress");
    }

    private static final long serialVersionUID = 1L;
}
