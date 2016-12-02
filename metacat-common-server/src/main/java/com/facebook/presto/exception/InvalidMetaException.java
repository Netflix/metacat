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
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;

/**
 * Exception when the given information about an entity is invalid.
 */
public class InvalidMetaException extends PrestoException {
    private SchemaTableName tableName;
    private String partitionId;

    /**
     * Constructor.
     * @param tableName table name
     * @param cause error cause
     */
    public InvalidMetaException(final SchemaTableName tableName, final Throwable cause) {
        super(StandardErrorCode.USER_ERROR, String.format("Invalid metadata for %s.", tableName), cause);
        this.tableName = tableName;
    }

    /**
     * Constructor.
     * @param tableName table name
     * @param partitionId partition name
     * @param cause error cause
     */
    public InvalidMetaException(final SchemaTableName tableName, final String partitionId, final Throwable cause) {
        super(StandardErrorCode.USER_ERROR,
            String.format("Invalid metadata for %s for partition %s.", tableName, partitionId), cause);
        this.tableName = tableName;
        this.partitionId = partitionId;
    }

    /**
     * Constructor.
     * @param message error message
     * @param cause error cause
     */
    public InvalidMetaException(final String message, final Throwable cause) {
        super(StandardErrorCode.USER_ERROR, message, cause);
    }
}
