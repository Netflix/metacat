package com.netflix.metacat.common.exception;

import javax.ws.rs.core.Response;

/**
 * TODO: This should be replaced by a BadRequestException from JAX-RS 2.x once we support the newer JAX-RS version.
 */
public class MetacatUserMetadataException extends MetacatException {
    public MetacatUserMetadataException(String reason) {
        super(reason, Response.Status.SEE_OTHER, null);
    }
}
