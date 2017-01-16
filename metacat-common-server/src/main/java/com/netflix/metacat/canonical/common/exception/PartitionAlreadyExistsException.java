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

package com.netflix.metacat.canonical.common.exception;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.netflix.metacat.canonical.common.spi.SchemaTableName;

import java.util.List;

/**
 * Exception when partition already exists.
 * @author zhenl
 */
public class PartitionAlreadyExistsException extends MetacatException {
    private static final Joiner COMMA_JOINER = Joiner.on(',');

    /**
     * Constructor.
     *
     * @param tableName   table name
     * @param partitionId partition name
     */
    public PartitionAlreadyExistsException(final SchemaTableName tableName, final String partitionId) {
        this(tableName, partitionId, null);
    }

    /**
     * Constructor.
     *
     * @param tableName   table name
     * @param partitionId partition name
     * @param cause       error cause
     */
    public PartitionAlreadyExistsException(final SchemaTableName tableName, final String partitionId,
                                           final Throwable cause) {
        super(StandardErrorCode.ALREADY_EXISTS,
            String.format("Partition '%s' already exists for table '%s'", Strings.nullToEmpty(partitionId), tableName),
            cause);
    }

    /**
     * Constructor.
     *
     * @param tableName    table name
     * @param partitionIds partition names
     * @param cause        error cause
     */
    public PartitionAlreadyExistsException(final SchemaTableName tableName, final List<String> partitionIds,
                                           final Throwable cause) {
        super(StandardErrorCode.ALREADY_EXISTS,
            String.format("One or more of the partitions '%s' already exists for table '%s'",
                COMMA_JOINER.join(partitionIds), tableName), cause);
    }
}
