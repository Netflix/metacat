/*
 *
 *  Copyright 2016 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.metacat.common.server.connectors.exception;

import javax.annotation.Nullable;

/**
 * Connector exception class.
 *
 * @author zhenl
 */
public class ConnectorException extends RuntimeException {

    /**
     * Constructor.
     *
     * @param message message
     */
    public ConnectorException(final String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message message
     * @param cause   cause
     */
    public ConnectorException(final String message, @Nullable final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor.
     *
     * @param message            message
     * @param cause              cause
     * @param enableSuppression  enable suppression
     * @param writableStackTrace stacktrace
     */
    public ConnectorException(
        final String message,
        @Nullable final Throwable cause,
        final boolean enableSuppression,
        final boolean writableStackTrace
    ) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getMessage() {
        String message = super.getMessage();
        if (message == null && getCause() != null) {
            message = getCause().getMessage();
        }
        return message;
    }
}
