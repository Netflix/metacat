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

package com.facebook.presto.exception;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;

/**
 * Exception when schema already exists.
 */
public class SchemaAlreadyExistsException extends PrestoException {
    private final String schemaName;

    /**
     * Constructor.
     * @param schemaName schema name
     */
    public SchemaAlreadyExistsException(final String schemaName) {
        this(schemaName, null);
    }

    /**
     * Constructor.
     * @param schemaName schema name
     * @param cause error cause
     */
    public SchemaAlreadyExistsException(final String schemaName, final Throwable cause) {
        super(StandardErrorCode.ALREADY_EXISTS, String.format("Schema %s already exists.", schemaName), cause);
        this.schemaName = schemaName;
    }
}
