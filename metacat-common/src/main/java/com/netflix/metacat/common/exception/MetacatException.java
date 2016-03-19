package com.netflix.metacat.common.exception;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.json.MetacatJsonLocator;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by amajumdar on 3/27/15.
 */
public class MetacatException extends WebApplicationException {
    private static final MetacatJson metacatJson = MetacatJsonLocator.INSTANCE;
    private static final ObjectNode EMPTY_ERROR = metacatJson.emptyObjectNode().put("error", "");

    /**
     * Construct a new client error exception.
     *
     * @param status client error status. Must be a {@code 4xx} status code.
     * @throws IllegalArgumentException in case the status code is {@code null} or is not from
     *                                  {@link javax.ws.rs.core.Response.Status.Family#CLIENT_ERROR} status code
     *                                  family.
     */
    public MetacatException(Response.Status status) {
        this(Response.status(status).type(MediaType.APPLICATION_JSON_TYPE).entity(EMPTY_ERROR).build(), null);
    }

    /**
     * Construct a new client error exception.
     *
     * @param status client error status. Must be a {@code 4xx} status code.
     * @throws IllegalArgumentException in case the status code is not a valid HTTP status code or
     *                                  if it is not from the {@link javax.ws.rs.core.Response.Status.Family#CLIENT_ERROR}
     *                                  status code family.
     */
    public MetacatException(int status) {
        this(Response.status(status).type(MediaType.APPLICATION_JSON_TYPE).entity(EMPTY_ERROR).build(), null);
    }

    /**
     * Construct a new client error exception.
     *
     * @param message the detail message (which is saved for later retrieval
     *                by the {@link #getMessage()} method).
     * @param status  client error status. Must be a {@code 4xx} status code.
     * @param cause   the underlying cause of the exception.
     * @throws IllegalArgumentException in case the status code is {@code null} or is not from
     *                                  {@link javax.ws.rs.core.Response.Status.Family#CLIENT_ERROR} status code
     *                                  family.
     */
    public MetacatException(String message, Response.Status status, Throwable cause) {
        this(
                Response.status(status)
                        .type(MediaType.APPLICATION_JSON_TYPE)
                        .entity(metacatJson.emptyObjectNode().put("error", message))
                        .build(),
                cause
        );
    }

    /**
     * Construct a new client error exception.
     *
     * @param message the detail message (which is saved for later retrieval
     *                by the {@link #getMessage()} method).
     * @param status  client error status. Must be a {@code 4xx} status code.
     * @param cause   the underlying cause of the exception.
     * @throws IllegalArgumentException in case the status code is not a valid HTTP status code or
     *                                  if it is not from the {@link javax.ws.rs.core.Response.Status.Family#CLIENT_ERROR}
     *                                  status code family.
     */
    public MetacatException(String message, int status, Throwable cause) {
        this(Response.status(status)
                        .type(MediaType.APPLICATION_JSON_TYPE)
                        .entity(metacatJson.emptyObjectNode().put("error", message))
                        .build(),
                cause);
    }

    /**
     * Construct a new client error exception.
     *
     * @param response client error response. Must have a status code set to a {@code 4xx}
     *                 status code.
     * @param cause    the underlying cause of the exception.
     * @throws IllegalArgumentException in case the response status code is not from the
     *                                  {@link javax.ws.rs.core.Response.Status.Family#CLIENT_ERROR} status code family.
     */
    public MetacatException(Response response, Throwable cause) {
        super(cause, response);
    }
}
