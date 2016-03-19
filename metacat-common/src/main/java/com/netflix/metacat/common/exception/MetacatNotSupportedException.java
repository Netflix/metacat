package com.netflix.metacat.common.exception;

import javax.ws.rs.core.Response;

/**
 * Created by amajumdar on 3/30/15.
 */
public class MetacatNotSupportedException extends MetacatException {
    public MetacatNotSupportedException() {
        this(null);
    }

    public MetacatNotSupportedException(String message) {
        super(message, Response.Status.UNSUPPORTED_MEDIA_TYPE, null);
    }
}
