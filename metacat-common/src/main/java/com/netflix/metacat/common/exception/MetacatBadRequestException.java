package com.netflix.metacat.common.exception;

import javax.ws.rs.core.Response;

/**
 * TODO: This should be replaced by a BadRequestException from JAX-RS 2.x once we support the newer JAX-RS version.
 */
public class MetacatBadRequestException extends MetacatException {
    public MetacatBadRequestException(String reason) {
        super(reason, Response.Status.BAD_REQUEST, null);
    }
}
