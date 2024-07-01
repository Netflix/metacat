package com.netflix.metacat.common.server.usermetadata;

/**
 * Parent Child Rel Service exception.
 */
public class ParentChildRelServiceException extends RuntimeException {
    /**
     * Constructor.
     *
     * @param m message
     */
    public ParentChildRelServiceException(final String m) {
        super(m);
    }

    /**
     * Constructor.
     *
     * @param m message
     * @param e exception
     */
    public ParentChildRelServiceException(final String m, final Exception e) {
        super(m, e);
    }
}
