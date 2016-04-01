/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
                cause == null? new Exception(message): cause
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
                cause == null? new Exception(message): cause);
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
