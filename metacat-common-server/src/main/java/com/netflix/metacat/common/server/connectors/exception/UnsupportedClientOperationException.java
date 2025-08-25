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

package com.netflix.metacat.common.server.connectors.exception;

import com.netflix.metacat.common.QualifiedName;

import javax.annotation.Nullable;

/**
 * Exception thrown when a client operation is not supported due to client version incompatibility.
 * For example, when an older Iceberg client tries to modify a table with branches or tags.
 *
 * @author gtret
 */
public class UnsupportedClientOperationException extends ConnectorException {
    /**
     * Constructor.
     *
     * @param name table name
     * @param message error message
     */
    public UnsupportedClientOperationException(final QualifiedName name, final String message) {
        super(message, null);
    }

    /**
     * Constructor.
     *
     * @param name table name
     * @param message error message
     * @param cause error cause
     */
    public UnsupportedClientOperationException(
        final QualifiedName name,
        final String message,
        @Nullable final Throwable cause
    ) {
        super(message, cause);
    }
}

