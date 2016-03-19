package com.netflix.metacat.common.exception;

import javax.ws.rs.core.Response;

/**
 * TODO: This should be replaced by a NotFoundException from JAX-RS 2.x once we support the newer JAX-RS version.
 */
public class MetacatNotFoundException extends MetacatException {
    public MetacatNotFoundException(String message) {
        super(message, Response.Status.NOT_FOUND, null);
    }
}
