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

import com.netflix.metacat.common.QualifiedName;

import javax.annotation.Nullable;

/**
 * Exception when table is not found.
 *
 * @author amajumdar
 */
public class TableNotFoundException extends NotFoundException {
    /**
     * Constructor.
     *
     * @param name qualified name of the table
     */
    public TableNotFoundException(final QualifiedName name) {
        super(name);
    }

    /**
     * Constructor.
     *
     * @param name  qualified name of the table
     * @param cause error cause
     */
    public TableNotFoundException(final QualifiedName name, @Nullable final Throwable cause) {
        super(name, cause);
    }

    /**
     * Constructor.
     *
     * @param name               qualified name of the table
     * @param cause              error cause
     * @param enableSuppression  enable suppression of the stacktrace
     * @param writableStackTrace writable stacktrace
     */
    public TableNotFoundException(
        final QualifiedName name,
        final Throwable cause,
        final boolean enableSuppression,
        final boolean writableStackTrace
    ) {
        super(name, cause, enableSuppression, writableStackTrace);
    }
}
