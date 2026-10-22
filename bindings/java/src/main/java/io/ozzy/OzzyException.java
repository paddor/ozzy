package io.ozzy;

/** Rust SDK failure. An admitted write can have an unknown outcome. */
public class OzzyException extends RuntimeException {
    /**
     * Construct a failure with its native diagnostic.
     *
     * @param message native diagnostic text
     */
    public OzzyException(String message) {
        super(message);
    }

    /**
     * Construct a failure with its cause.
     *
     * @param message diagnostic text
     * @param cause underlying failure
     */
    public OzzyException(String message, Throwable cause) {
        super(message, cause);
    }

    private static final long serialVersionUID = 1L;
}
