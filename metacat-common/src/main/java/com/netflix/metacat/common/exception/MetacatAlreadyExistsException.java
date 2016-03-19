package com.netflix.metacat.common.exception;

import javax.ws.rs.core.Response;

/**
 * Created by amajumdar on 5/11/15.
 */
public class MetacatAlreadyExistsException extends MetacatException{
    public MetacatAlreadyExistsException(String message) {
        super(message, Response.Status.CONFLICT, null);
    }
}
